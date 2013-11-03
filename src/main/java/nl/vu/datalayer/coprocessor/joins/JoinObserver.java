package nl.vu.datalayer.coprocessor.joins;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;

import nl.vu.datalayer.coprocessor.schema.PrefixMatchSchema;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

public class JoinObserver extends BaseRegionObserver{
	
	private static final int S = 0x08;
	private static final int P = 0x04;
	private static final int O = 0x02;
	private static final int C = 0x01;
	
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
	private int [] tableOffsets = null;
	private int tableIndex;
	
	public JoinObserver() {
		super();
		joinPositionsMap = new HashMap<InternalScanner, TripleJoinInfo>();
	}

	@Override
	public void preOpen(ObserverContext<RegionCoprocessorEnvironment> e) {
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
		super.preOpen(e);
	}

	@Override
	public RegionScanner preScannerOpen(ObserverContext<RegionCoprocessorEnvironment> e, Scan scan, RegionScanner s) throws IOException {
		Map<byte [], NavigableSet<byte []>> familyMap = scan.getFamilyMap();
		NavigableSet<byte []> columns = familyMap.get(PrefixMatchSchema.COLUMN_FAMILY_BYTES);
		if (columns!=null){
			if (columns.size()!=1){
				logger.error("[JoinObserver] Expecting one column in the scanner");
				return super.preScannerOpen(e, scan, s);
			}
			
			byte[] colBytes = columns.iterator().next();
			if (colBytes.length > 0) {
				joinPositionsMap.put(s, new TripleJoinInfo(colBytes, scan.getStartRow()));
				//reset the column qualifier of the scan
				byte []newCol= PrefixMatchSchema.COLUMN_BYTES;
				columns.clear();
				columns.add(newCol);
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
		byte joinPosition = tripleJoinInfo.getJoinPosition();
		
		for (Result result : results) {
			byte[] rowKey = result.getRow();
			
			byte[][] nonJoinValues = new byte[tripleJoinInfo.getVariableIds().length][];
			byte[] joinKey = extractJoinKeyAndNonJoinValues(rowKey, joinPosition, tripleJoinInfo.getStartRowKeyPositions(), nonJoinValues);
			
			for (int i = 0; i < nonJoinValues.length; i++) {
				Put put = new Put(joinKey);
				put.add(PrefixMatchSchema.JOIN_COL_FAM_BYTES, 
						new byte[]{tripleJoinInfo.getVariableIds()[i]}, 
						nonJoinValues[i]);
				//TODO make the call to the JOIN table
			}
			
		}
		return super.postScannerNext(e, s, results, limit, hasMore);
	}

	private byte[] extractJoinKeyAndNonJoinValues(byte[] rowKey, byte joinPosition, byte startRowKeyPosition, byte[][] nonJoinValues) {
				
		byte objectTypeByte = rowKey[tableOffsets[2]];
		byte objectType = (byte)(objectTypeByte >> 7 & 1);
		
		byte []joinKey = allocJoinKey(joinPosition, objectType);
		
		int nonJoinIndex=0;
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

	private byte[] allocJoinKey(byte joinPosition, byte objectType) {
		byte[] joinKey;
		int joinKeyLength = Integer.bitCount(joinPosition&0x000000ff)*BASE_ID_SIZE;
		if ((joinPosition & (byte) O) == O &&
				objectType == NUMERICAL_TYPE) {//join on object
			joinKeyLength++;
			joinKey = new byte[joinKeyLength];	
		}
		else{
			joinKey = new byte[joinKeyLength];
		}
		return joinKey;
	}
	
	//ASSUMPTION: expecting the input qualifier bytes to be
	// - 1 byte - encoding of join positions
	// - 1 byte id - for each variable id in a non-join position (in SPOC order)
	// example: S ?p ?o - join by ?o
	//			S - rowkey
	//			joinByte - 0x02 (bit encoding of object position)
	//			id(?p) - 1 byte id which will be used in the join table
	private class TripleJoinInfo{
		private byte joinPosition;
		private byte[] variableIds;
		private byte[] startRow;
		private byte startRowKeyPositions;
		
		public TripleJoinInfo(byte[] inputQualifier, byte[] startRow) {
			super();
			this.joinPosition = inputQualifier[0];
			this.variableIds = Bytes.tail(inputQualifier, inputQualifier.length-1);
			this.startRow = startRow;
			byte rowLength = (byte)(startRow.length+1/BASE_ID_SIZE);
			startRowKeyPositions = KEY_ENCODINGS[tableIndex][rowLength];
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
		
	}
}
