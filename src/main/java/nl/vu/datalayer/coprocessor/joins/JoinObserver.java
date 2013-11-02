package nl.vu.datalayer.coprocessor.joins;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;

import nl.vu.datalayer.coprocessor.schema.PrefixMatchSchema;

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
	
	private Logger logger = Logger.getLogger("CoprocessorLog");
	
	private HashMap<InternalScanner, Pair<Byte, byte[]>> joinPositionsMap;
	private HashMap<String, int[]> tablesToOffsets;
	
	public static final int TYPED_ID_SIZE = 9;
	public static final int BASE_ID_SIZE = 8;
	
	public JoinObserver() {
		super();
		joinPositionsMap = new HashMap<InternalScanner, Pair<Byte, byte[]>>();
		tablesToOffsets = new HashMap<String, int[]>();
		
		for (int i = 0; i < PrefixMatchSchema.TABLE_NAMES.length; i++) {
			tablesToOffsets.put(PrefixMatchSchema.TABLE_NAMES[i], PrefixMatchSchema.OFFSETS[i]);
		}
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
			if (colBytes.length == 1) {
				joinPositionsMap.put(s, Pair.newPair(colBytes[0], scan.getStartRow()));
				//reset the column qualifier of the scan
				byte []newCol= PrefixMatchSchema.COLUMN_BYTES;
				columns.clear();
				columns.add(newCol);
				
				//TODO get variable names/ids from the engine
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
		
		byte joinPosition = joinPositionsMap.get(s).getFirst();
		
		for (Result result : results) {
			byte[] rowKey = result.getRow();
			
			byte[] joinKey = extractJoinKey(rowKey, joinPosition, e.getEnvironment().getRegion().getTableDesc().getNameAsString());
			//TODO create columns with corresponding variable names/ids
		}
		return super.postScannerNext(e, s, results, limit, hasMore);
	}

	private byte[] extractJoinKey(byte[] rowKey, byte joinPosition, String tableName) {
		int joinKeyLength = Integer.bitCount(joinPosition&0x000000ff)*BASE_ID_SIZE;
		byte []joinKey;
		if ((joinPosition & (byte) 0x02) == 1) {//join on object
			joinKeyLength++;
			joinKey = new byte[joinKeyLength];	
		}
		else{
			joinKey = new byte[joinKeyLength];
		}
		
		int currentOffset = 0;
		int []offsets = tablesToOffsets.get(tableName);
		if ((joinPosition & (byte) 0x08) == 1) {//join on subject
			Bytes.putBytes(joinKey, currentOffset, rowKey, offsets[0], BASE_ID_SIZE);
			currentOffset+=BASE_ID_SIZE;
		}
		if ((joinPosition & (byte) 0x04) == 1) {//join on predicate
			Bytes.putBytes(joinKey, currentOffset, rowKey, offsets[1], BASE_ID_SIZE);
			currentOffset+=BASE_ID_SIZE;
		}
		if ((joinPosition & (byte) 0x02) == 1) {//join on object
			Bytes.putBytes(joinKey, currentOffset, rowKey, offsets[2], TYPED_ID_SIZE);
		}
		
		return joinKey;
	}
	
	

}
