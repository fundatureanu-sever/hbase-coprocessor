package nl.vu.datalayer.coprocessor.joins;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;

import nl.vu.datalayer.coprocessor.schema.PrefixMatchSchema;

import org.apache.hadoop.hbase.client.HTable;
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

public class JoinObserver extends BaseRegionObserver{
	
	public static final int S = 0x08;
	public static final int P = 0x04;
	public static final int O = 0x02;
	public static final int C = 0x01;
	
	private static byte STRING_TYPE = 0;
	private static byte NUMERICAL_TYPE = 1;
	
	public static final byte[][]KEY_ENCODINGS = 
		{{0x00, S, S|P, S|P|O }, 
		{0x00, P, P|O, P|O,C }, 
		{0x00, O, O|S, O|S|P }, 
		{0x00, O, O|C, O|C|S }, 
		{0x00, C, C|S, C|S|P }, 
		{0x00, C, C|P, C|P|S }};

	private Logger logger = Logger.getLogger("CoprocessorLog");
	
	private HashMap<InternalScanner, TripleJoinInfo> joinPositionsMap;
	
	public static final int TYPED_ID_SIZE = 9;
	public static final int BASE_ID_SIZE = 8;
	public static int [] tableOffsets = null; //TODO remove static
	public static int tableIndex; //TODO remove static
	
	private HTable joinTable=null;
	
	public JoinObserver() {
		super();
		joinPositionsMap = new HashMap<InternalScanner, TripleJoinInfo>(); 
	}

	@Override
	public void preOpen(ObserverContext<RegionCoprocessorEnvironment> e) {
		try {
			joinTable = new HTable(e.getEnvironment().getConfiguration(), PrefixMatchSchema.JOIN_TABLE_NAME);
		} catch (IOException exc) {
			logger.error("Unable to initialize JOIN table: "+exc.getMessage());
		}
		
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
		
		logger.info("preOpen: Successful initialization");
		
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
				TripleJoinInfo tripleJoinInfo = new TripleJoinInfo(colBytes, scan.getStartRow());
				if (tripleJoinInfo.checkLengthParamters()==false){
					String errMessage = "[JoinObserver] preScannerOpen: Sum of lengths for prefix, join key and non-join ids != 3";
					logger.error(errMessage);
					throw new IOException(errMessage);
				}
				
				joinPositionsMap.put(s, tripleJoinInfo);
				//reset the column qualifier of the scan
				byte []newCol= PrefixMatchSchema.COLUMN_BYTES;
				columns.clear();
				columns.add(newCol);
				
				logger.info("[JoinObserver] preScannerOpen: Successfully reset column from coprocessor");
			}
		}		
		
		return super.preScannerOpen(e, scan, s);
	}

	@Override
	public boolean preScannerNext(
			ObserverContext<RegionCoprocessorEnvironment> e, InternalScanner s,
			List<Result> results, int limit, boolean hasMore)
			throws IOException {
		
		
		return super.preScannerNext(e, s, results, limit, hasMore);
	}

	@Override
	public boolean postScannerNext(
			ObserverContext<RegionCoprocessorEnvironment> e, InternalScanner s,
			List<Result> results, int limit, boolean hasMore)
			throws IOException {
		
		TripleJoinInfo tripleJoinInfo = joinPositionsMap.get(s);
		if (tripleJoinInfo==null){
			return super.postScannerNext(e, s, results, limit, hasMore);
		}
		logger.info("[JoinObserver] postScannerNext: Found the InternalScanner in the internal map");
		
		for (Result result : results) {
			byte[] rowKey = result.getRow();
			
			byte[][] nonJoinValues = new byte[tripleJoinInfo.getVariableIds().length][];
			byte[] joinKey = extractJoinKeyAndNonJoinValues(rowKey, tripleJoinInfo, nonJoinValues);
			
			for (int i = 0; i < nonJoinValues.length; i++) {
				Put put = new Put(joinKey);
				put.add(PrefixMatchSchema.JOIN_COL_FAM_BYTES, 
						new byte[]{tripleJoinInfo.getVariableIds()[i]}, 
						nonJoinValues[i]);
				joinTable.put(put);
				logger.info("[JoinObserver] postScannerNext: Successfully inserted in the join table");
			}
			
		}
		return super.postScannerNext(e, s, results, limit, hasMore);
	}

	public static byte[] extractJoinKeyAndNonJoinValues(byte[] rowKey, TripleJoinInfo tripleJoinInfo, byte[][] nonJoinValues) {	
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

	/*private static byte[] allocJoinKey(byte joinPosition, byte objectType) {
		byte[] joinKey;
		
		int joinKeyLength = Integer.bitCount(joinPosition&0x000000ff)*BASE_ID_SIZE;
		if ((joinPosition & (byte) O) == O &&
				objectType == NUMERICAL_TYPE) {//join on object
			joinKeyLength++;
			
		}
		else{
			joinKey = new byte[joinKeyLength];
		}
		return joinKey;
	}*/
	
	
	
	
	
	public static void main(String[] args) {
		tableIndex = PrefixMatchSchema.CSPO;
		tableOffsets = PrefixMatchSchema.OFFSETS[tableIndex];
		int variablesLength=1;
		byte[][] nonJoinValues = new byte[variablesLength][];
		
		byte[] inputQualifier = { O|P,//JOIN KEY,
								(byte)0xf8/*, (byte)0xf5*/ };
		
		byte[] startRow = new byte[]{};//0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01};
		TripleJoinInfo joinInfo = new TripleJoinInfo(inputQualifier, startRow);
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
		
	}
	
	

	@Override
	public void postClose(ObserverContext<RegionCoprocessorEnvironment> e,
			boolean abortRequested) {
		try {
			if (joinTable==null){
				joinTable.close();
			}
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		super.postClose(e, abortRequested);
	}

	public static String hexaString(byte []b, int offset, int length){
		String ret = "";
		for (int i = offset; i < length; i++) {
			ret += String.format("\\x%02x", b[i]);
		}
		return ret;
	}
	
	//ASSUMPTION: expecting the input qualifier bytes to be
	// - 1 byte - encoding of join positions
	// - 1 byte id - for each variable id in a non-join position (in SPOC order)
	// example: S ?p ?o - join by ?o
	//			S - rowkey
	//			joinByte - 0x02 (bit encoding of object position)
	//			id(?p) - 1 byte id which will be used in the join table
	private static class TripleJoinInfo{
		private static final int TRIPLE_SIZE = 3;
		
		private byte joinPosition;
		private byte[] variableIds;
		private byte[] startRow;
		private byte startRowKeyPositions;
		private byte startRowNumberOfElems;
		private int joinKeyLength;
		private int joinNumberOfElems;

		//private byte objectType;

		public TripleJoinInfo(byte[] inputQualifier, byte[] startRow) {
			super();
			this.joinPosition = inputQualifier[0];
			this.variableIds = Bytes.tail(inputQualifier, inputQualifier.length-1);
			this.startRow = startRow;
			startRowNumberOfElems = (byte)((startRow.length+1)/BASE_ID_SIZE);
			startRowKeyPositions = KEY_ENCODINGS[tableIndex][startRowNumberOfElems];
			
			joinNumberOfElems = bitCount(joinPosition);
			joinKeyLength = joinNumberOfElems*BASE_ID_SIZE;			
		}
		
		private int bitCount(byte b){
			int count = 0;
			for (int i = 0; i < 4; i++) {
				if ( (b & 1) == 1){
					count++;
				}
				b = (byte)(b>>1);
			}
			
			return count;
		}
		
		public boolean checkLengthParamters(){
			return (joinNumberOfElems+startRowNumberOfElems+variableIds.length==TRIPLE_SIZE);
		}
		
		public byte getJoinPosition(){
			return joinPosition;
		}
		
		public byte[] getVariableIds(){
			return variableIds;
		}
		
		public byte[] getStartRow(){
			return startRow;
		}
		
		public byte getStartRowKeyPositions(){
			return startRowKeyPositions;
		}
		
		public int getJoinKeyLength() {
			return joinKeyLength;
		}
		
		public void adjustJoinKeyLength(byte objectType){
			if ((joinPosition & (byte) O) == O) {			
				if (objectType == NUMERICAL_TYPE) {//join on object
					joinKeyLength++;
				}
			}	
		}
		
	}
	
	
}
