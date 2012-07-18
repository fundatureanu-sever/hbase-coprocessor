package nl.vu.datalayer.hbase.coprocessor;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;

import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;

public class PrefixMatchSecondaryIndexTest {
	
	private static final int PUTS_LIMIT = 300;
	private CoprocessorEnvironment env;
	private PrefixMatchSecondaryIndex coprocessor;
	private HTable [] tables;
	
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
			}
			
		} catch (IOException e1) {
			e1.printStackTrace();
			assertTrue(false);
		}
		
		System.out.println("Sleeping for 1 second");
		try {
			Thread.sleep(1000);//sleep for a while to allow for puts to be called
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		System.out.println("Waking up");
		
		for (int i = 0; i < tables.length; i++) {
			try {
				verify(tables[i], times(PUTS_LIMIT)).put((Put)Matchers.notNull());
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		}
	}

	private void createCoprocessorWith3Tables() {	
		int tablesNumber = 3;
		tables = new HTable[tablesNumber-1];
		for (int i = 0; i < tables.length; i++) {
			tables[i] = mock(HTable.class);
		}
		
		coprocessor = new PrefixMatchSecondaryIndex("_TestSuffix", true, tables);
	}
	
	/*@Test
	public void multiThreadedPrePutTest(){
		createCoprocessorWith3Tables();
		
		int nThreads = 8;
		int previous = 0;
		int coprocessorThreadPortion = 41200;
		
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
	
	@Test 
	public void repeatMulti(){
		for (int i = 0; i < 200; i++) {
			multiThreadedPrePutTest();
		}
	}
	*/
}
