package nl.vu.datalayer.hbase.coprocessor;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

public class CoprocessorTestThread extends Thread {

	private PrefixMatchSecondaryIndex coprocessor;
	private long start;
	private long end;

	public CoprocessorTestThread(PrefixMatchSecondaryIndex coprocessor, long start, long end) {
		super();
		this.coprocessor = coprocessor;
		this.start = start;
		this.end = end;
	}

	@Override
	public void run() {
		ObserverContext<RegionCoprocessorEnvironment> e = mock(ObserverContext.class);
		
		try{
			for (int i = 0; i < (end - start); i++) {
				byte[] id = Bytes.toBytes(start + i);
				byte[] key = Bytes.padHead(id, 33);
				assertTrue(key.length != 33);
				Put put = new Put(key);
				put.add("F".getBytes(), null, null);

				coprocessor.prePut(e, put, new WALEdit(), false);
			}
			
		} catch (IOException e1) {
			e1.printStackTrace();
			assertTrue(false);
		}
	}
	
}
