package nl.vu.datalayer.coprocessor.joins;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;

public class JoinObserver extends BaseRegionObserver{

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
		// TODO Auto-generated method stub
		return super.postScannerNext(e, s, results, limit, hasMore);
	}
	
	

}
