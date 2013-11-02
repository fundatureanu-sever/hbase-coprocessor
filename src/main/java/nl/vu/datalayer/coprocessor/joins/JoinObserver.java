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
import org.apache.log4j.Logger;

public class JoinObserver extends BaseRegionObserver{
	
	private Logger logger = Logger.getLogger("CoprocessorLog");
	
	private HashMap<InternalScanner, Byte> joinPositionsMap;
	
	
	public JoinObserver() {
		super();
		joinPositionsMap = new HashMap<InternalScanner, Byte>();
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
				joinPositionsMap.put(s, colBytes[0]);
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
		
		byte joinPosition = joinPositionsMap.get(s);
		switch (joinPosition){
			
		}
		return super.postScannerNext(e, s, results, limit, hasMore);
	}
	
	

}
