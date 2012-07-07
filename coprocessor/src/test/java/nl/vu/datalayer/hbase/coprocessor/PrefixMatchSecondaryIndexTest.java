package nl.vu.datalayer.hbase.coprocessor;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.WritableByteArrayComparable;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class PrefixMatchSecondaryIndexTest {
	
	private static final int PUTS_LIMIT = 300;
	private CoprocessorEnvironment env;
	private PrefixMatchSecondaryIndex coprocessor;
	private HTableInterface [] tables;
	
	@Before
	public void runBeforeEveryTest() {
		 env = mock(CoprocessorEnvironment.class);
	}
	
	@After
	public void runAfterEveryTest() {
		 try {
			coprocessor.stop(env);
		} catch (IOException e) {
			e.printStackTrace();
			assertTrue(false);
		}
	}
	
	@Test
	public void testPrePut() {
		createCoprocessorWith3Tables();
		
		ObserverContext<RegionCoprocessorEnvironment> e = mock(ObserverContext.class);
		
		try {
			long start = 0;
			
			for (int i = 0; i < PUTS_LIMIT; i++) {
				byte []id = Bytes.toBytes(start+i);
				byte []key = Bytes.padHead(id, 33);
				assertTrue(key.length != 33);
				Put put = new Put(key);
				put.add("F".getBytes(), null, null);
				
				coprocessor.prePut(e, put, new WALEdit(), false);
				
				ArrayList<CoprocessorDoubleBuffer> retBatchPuts = coprocessor.getBatchPuts();
				for (int j = 0; j < retBatchPuts.size(); j++) {
					assertTrue(retBatchPuts.get(j).getCurrentBuffer().size() == (i+1)%PrefixMatchSecondaryIndex.FLUSH_LIMIT);
					assertTrue(retBatchPuts.get(j).getNextBuffer().size() == 0);
				}
			}
			
		} catch (IOException e1) {
			e1.printStackTrace();
			assertTrue(false);
		}
	}

	private void createCoprocessorWith3Tables() {
		ArrayList<CoprocessorDoubleBuffer> batchPuts = new ArrayList<CoprocessorDoubleBuffer>();
		
		int tablesNumber = 3;
		tables = new HTableInterface[tablesNumber-1];
		for (int i = 0; i < tables.length; i++) {
			CoprocessorDoubleBuffer newDB = new CoprocessorDoubleBuffer();
			batchPuts.add(newDB);
			tables[i] = mock(HTable.class);
		}
		
		coprocessor = new PrefixMatchSecondaryIndex("_TestSuffix", true, tables, batchPuts, tablesNumber);
		 try {
			coprocessor.start(env);
		} catch (IOException e) {
			e.printStackTrace();
			assertTrue(false);
		}
	}
	
	@Test
	public void multiThreadedPrePutTest(){
		createCoprocessorWith3Tables();
		
		int nThreads = 3;
		int previous = 0;
		int coprocessorThreadPortion = 30000;
		
		CoprocessorTestThread []testThreads = new CoprocessorTestThread[nThreads];
		for (int i = 0; i < nThreads; i++) {
			testThreads[i] = new CoprocessorTestThread(coprocessor, previous, previous+coprocessorThreadPortion);
			previous+=coprocessorThreadPortion;
			testThreads[i].start();
		}
		
		try {
			for (int i = 0; i < testThreads.length; i++) {
				testThreads[i].join();
			}
					
			ObserverContext<RegionCoprocessorEnvironment> e = mock(ObserverContext.class);
			coprocessor.preCheckAndPut(e, null, null, "c".getBytes(), 
					CompareOp.EQUAL, 
					null, 
					null, false);
		} catch (InterruptedException e) {
			e.printStackTrace();
			assertTrue(false);
		} catch (IOException e) {
			e.printStackTrace();
			assertTrue(false);
		}
		
		System.out.println("Count: "+coprocessor.putCounter);
		assertTrue(coprocessor.putCounter.get() == coprocessorThreadPortion*nThreads*tables.length);
	}
	
	

}
