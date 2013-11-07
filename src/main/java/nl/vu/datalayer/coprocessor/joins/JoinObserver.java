package nl.vu.datalayer.coprocessor.joins;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;

import nl.vu.datalayer.coprocessor.schema.PrefixMatchSchema;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.sematext.hbase.wd.AbstractRowKeyDistributor;
import com.sematext.hbase.wd.RowKeyDistributorByHashPrefix;

public class JoinObserver extends BaseRegionObserver{
	
	public static final int S = 0x08;
	public static final int P = 0x04;
	public static final int O = 0x02;
	public static final int C = 0x01;
	
	public static final byte[][]KEY_ENCODINGS = 
		{{0x00, S, S|P, S|P|O }, 
		{0x00, P, P|O, P|O,C }, 
		{0x00, O, O|S, O|S|P }, 
		{0x00, O, O|C, O|C|S }, 
		{0x00, C, C|S, C|S|P }, 
		{0x00, C, C|P, C|P|S }};
	
	public static byte STRING_TYPE = 0;
	public static byte NUMERICAL_TYPE = 1;
	
	private Logger logger = Logger.getLogger("CoprocessorLog");
	
	private HashMap<InternalScanner, MetaInfo> metaInfoMap;
	private HashMap<Scan, MetaInfo> tempMetaInfoMap;
	
	public static final int TYPED_ID_SIZE = 9;
	public static final int BASE_ID_SIZE = 8;
	
	private static final int BUCKET_COUNT = 8;//TODO should be a parameter
	public int [] tableOffsets = null; //add this for testing in main
	public int tableIndex; //add this for testing in main
	
	
	private AbstractRowKeyDistributor keyDistributor;
	private HTablePool tablePool;
	
	public JoinObserver() {
		super();
		metaInfoMap = new HashMap<InternalScanner, MetaInfo>(); 
		tempMetaInfoMap = new HashMap<Scan, MetaInfo>();
		//logger.info("Observer contructor called");
	}

	@Override
	public void preOpen(ObserverContext<RegionCoprocessorEnvironment> e) {
		//tablePool = new HTablePool(e.getEnvironment().getConfiguration(), 25, PoolType.ThreadLocal);	
		
		String currentTable = e.getEnvironment().getRegion().getTableDesc().getNameAsString();	
		
		for (int i = 0; i < PrefixMatchSchema.TABLE_NAMES.length; i++) {
			if (PrefixMatchSchema.TABLE_NAMES[i].equals(currentTable)){
				tableOffsets = PrefixMatchSchema.OFFSETS[i];
				tableIndex = i;
				break;
			}
		}
		
		if (tableOffsets==null){
			logger.error("Could not initialize tableOffsets for the table: "+currentTable);
		}
		
		keyDistributor = new RowKeyDistributorByHashPrefix(new RowKeyDistributorByHashPrefix.OneByteSimpleHash(BUCKET_COUNT));
		
		logger.info("preOpen: Successful initialization "+currentTable+" "+this.toString());
		
		super.preOpen(e);
	}

	@Override
	public RegionScanner preScannerOpen(ObserverContext<RegionCoprocessorEnvironment> e, Scan scan, RegionScanner s) throws IOException {
		Map<byte [], NavigableSet<byte []>> familyMap = scan.getFamilyMap();
		NavigableSet<byte []> columns = familyMap.get(PrefixMatchSchema.COLUMN_FAMILY_BYTES);
		if (columns!=null){
			if (columns.size()!=1){
				logger.info("[JoinObserver] Expecting one column in the scanner");
				return super.preScannerOpen(e, scan, s);
			}
			
			byte[] colBytes = columns.iterator().next();
			if (colBytes.length > 0) {
				logger.info(PrefixMatchSchema.TABLE_NAMES[tableIndex]+" preScannerOpen: inputQualifier: "+hexaString(colBytes, 0, colBytes.length)+
									" startRowKey: "+hexaString(scan.getStartRow(), 0, scan.getStartRow().length));
				MetaInfo metaInfo = new MetaInfo(colBytes, scan.getStartRow(), tableIndex);
				if (metaInfo.checkLengthParamters()==false){
					String errMessage = "[JoinObserver] preScannerOpen: Sum of lengths for prefix, join key and non-join ids != 3";
					logger.error(errMessage);
					throw new IOException(errMessage);
				}
				
				tempMetaInfoMap.put(scan, metaInfo);
				//reset the column qualifier of the scan
				byte []newCol= PrefixMatchSchema.COLUMN_BYTES;
				columns.clear();
				columns.add(newCol);
				
				logger.info(PrefixMatchSchema.TABLE_NAMES[tableIndex]+"[JoinObserver] preScannerOpen: Successfully reset column from coprocessor");
			}
		}		
		
		return super.preScannerOpen(e, scan, s);
	}

	@Override
	public RegionScanner postScannerOpen(ObserverContext<RegionCoprocessorEnvironment> e, Scan scan, RegionScanner s) throws IOException {
		MetaInfo metaInfo = tempMetaInfoMap.get(scan);
		if (metaInfo!=null){
			metaInfoMap.put(s, metaInfo);
			tempMetaInfoMap.remove(scan);
		}
		else{
			logger.error("[JoinObserver] postScannerOpen: Could not pass meta info forward");
		}
		return super.postScannerOpen(e, scan, s);
	}

	@Override
	public boolean postScannerNext(
			ObserverContext<RegionCoprocessorEnvironment> e, InternalScanner s,
			List<Result> results, int limit, boolean hasMore)
			throws IOException {
		
		MetaInfo metaInfo = metaInfoMap.get(s);
		if (metaInfo==null){
			logger.info("[JoinObserver] postScannerNext: InternalScanner not found in the internal map "+s.toString());
			return super.postScannerNext(e, s, results, limit, hasMore);
		}
		//logger.info("[JoinObserver] postScannerNext: Found the InternalScanner in the internal map");
		
		HTableInterface joinTable = e.getEnvironment().getTable(PrefixMatchSchema.JOIN_TABLE_NAME.getBytes());
		//joinTable.setAutoFlush(false);
		ArrayList<Mutation> writes = new ArrayList<Mutation>(results.size());
		
		for (Result result : results) {
			byte[] rowKey = result.getRow();
			
			byte[][] nonJoinValues = new byte[metaInfo.getVariableIds().length][];
			byte[] joinKey = extractJoinKeyAndNonJoinValues(rowKey, metaInfo, nonJoinValues);
			byte []joinIdBytes = Bytes.toBytes(metaInfo.getJoinId());			
			byte[] distributedKey = keyDistributor.getDistributedKey(Bytes.add(joinIdBytes,joinKey));
			
			for (int i = 0; i < nonJoinValues.length; i++) {
				Append append = new Append(distributedKey);	
				append.add(PrefixMatchSchema.JOIN_COL_FAM_BYTES, 
						new byte[]{metaInfo.getTripleId(),metaInfo.getVariableIds()[i]}, 
						nonJoinValues[i]);
				
				writes.add(append);
				logger.info(PrefixMatchSchema.TABLE_NAMES[tableIndex]+"[JoinObserver] postScannerNext: Successfully inserted in the join table a non join value");
			}
			
			if (nonJoinValues.length==0){//we add this column just to know that it was part of the join
				Put put = new Put(distributedKey);
				put.add(PrefixMatchSchema.JOIN_COL_FAM_BYTES, 
						new byte[]{metaInfo.getTripleId()}, new byte[]{0x00});
				writes.add(put);
				logger.info(PrefixMatchSchema.TABLE_NAMES[tableIndex]+"[JoinObserver] postScannerNext: Successfully inserted in the join table an empty non join value");
			}	
		}
		
		try {
			joinTable.batch(writes);
		} catch (InterruptedException e1) {
			throw new IOException("Problem writing to JOIN table: "+e1.getMessage());
		}
		
		if (results!=null && results.size()>0){
			results.clear();
			results.add(new Result(new KeyValue[]{KeyValue.LOWESTKEY}));//add dummy just to keep the scanner going
		}
		
		return super.postScannerNext(e, s, results, limit, hasMore);
	}

	public byte[] extractJoinKeyAndNonJoinValues(byte[] rowKey, MetaInfo tripleJoinInfo, byte[][] nonJoinValues) {	
		byte startRowKeyPosition = tripleJoinInfo.getStartRowKeyPositions();
		byte joinPosition = tripleJoinInfo.getJoinPosition();
		
		byte objectTypeByte = rowKey[tableOffsets[2]];
		byte objectType = (byte) (objectTypeByte >> 7 & 1);
		tripleJoinInfo.adjustJoinKeyLength(objectType);
		byte []joinKey = new byte[tripleJoinInfo.getJoinKeyLength()];
		
		int nonJoinIndex = 0;
		int joinKeyOffset = 0;
		
		if ((joinPosition & (byte) S) == S) {//join on subject
			Bytes.putBytes(joinKey, joinKeyOffset, rowKey, tableOffsets[0], BASE_ID_SIZE);
			joinKeyOffset+=BASE_ID_SIZE;
		}
		else if ((startRowKeyPosition & (byte)S) == 0){//if it's not part of the start row key
			nonJoinValues[nonJoinIndex] = new byte[BASE_ID_SIZE];
			Bytes.putBytes(nonJoinValues[nonJoinIndex], 0, rowKey, tableOffsets[0], BASE_ID_SIZE);
			nonJoinIndex++;
		}

		if ((joinPosition & (byte) P) == P) {//join on predicate
			Bytes.putBytes(joinKey, joinKeyOffset, rowKey, tableOffsets[1], BASE_ID_SIZE);
			joinKeyOffset+=BASE_ID_SIZE;
		}
		else if ((startRowKeyPosition & (byte)P) == 0){
			nonJoinValues[nonJoinIndex] = new byte[BASE_ID_SIZE];
			Bytes.putBytes(nonJoinValues[nonJoinIndex], 0, rowKey, tableOffsets[1], BASE_ID_SIZE);
			nonJoinIndex++;
		}
		
		if ((joinPosition & (byte) O) == O) {//join on object
			if (objectType == STRING_TYPE) {
				Bytes.putBytes(joinKey, joinKeyOffset, rowKey, tableOffsets[2] + 1, BASE_ID_SIZE);
			}
			else{//numerical
				Bytes.putBytes(joinKey, joinKeyOffset, rowKey, tableOffsets[2], TYPED_ID_SIZE);
			}			
		}
		else if ((startRowKeyPosition & (byte) O) == 0) {
			if (objectType == STRING_TYPE) {
				nonJoinValues[nonJoinIndex] = new byte[BASE_ID_SIZE];
				Bytes.putBytes(nonJoinValues[nonJoinIndex], 0, rowKey, tableOffsets[2] + 1, BASE_ID_SIZE);
			} else {//numerical
				nonJoinValues[nonJoinIndex] = new byte[TYPED_ID_SIZE];
				Bytes.putBytes(nonJoinValues[nonJoinIndex], 0, rowKey, tableOffsets[2], TYPED_ID_SIZE);
			}
		}
		
		return joinKey;
	}
	
	/*public static void main(String[] args) {
		tableIndex = PrefixMatchSchema.CSPO;
		tableOffsets = PrefixMatchSchema.OFFSETS[tableIndex];
		int variablesLength=1;
		byte[][] nonJoinValues = new byte[variablesLength][];
		
		byte[] inputQualifier = { O|P,//JOIN KEY,
								(byte)0xf8/*, (byte)0xf5 };
		
		byte[] startRow = new byte[]{};//0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01};
		MetaInfo joinInfo = new MetaInfo(inputQualifier, startRow, tableIndex);
		if (joinInfo.checkLengthParamters()==false){
			System.err.println("Cacat");
			System.exit(-1);
		}
		
		byte[] rowKey = new byte[]{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,
				(byte)0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04};
		
		byte[] joinKey = extractJoinKeyAndNonJoinValues(rowKey, joinInfo, nonJoinValues);
		
		System.out.println("Join Key: "+hexaString(joinKey, 0, joinKey.length));
		for (byte[] nonJ : nonJoinValues) {
			System.out.println("Non Join Value: "+hexaString(nonJ, 0, nonJ.length));
		}		
	}*/

	/*@Override
	public void postScannerClose(ObserverContext<RegionCoprocessorEnvironment> e, InternalScanner s) throws IOException {
		if (joinTable!=null){
			//joinTable.flushCommits();
		}
		super.postScannerClose(e, s);
	}

	@Override
	public void postClose(ObserverContext<RegionCoprocessorEnvironment> e,
			boolean abortRequested) {
		/*try {
			if (joinTable!=null){
				joinTable.close();
			}
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		super.postClose(e, abortRequested);
	}*/

	public static String hexaString(byte []b, int offset, int length){
		String ret = "";
		for (int i = offset; i < length; i++) {
			ret += String.format("\\x%02x", b[i]);
		}
		return ret;
	}
	
}
