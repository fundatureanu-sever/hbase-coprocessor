package nl.vu.datalayer.hbase.coprocessor;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

public class PrefixMatchGenerateSecondaryIndex extends BaseEndpointCoprocessor
										implements PrefixMatchProtocol{

	private static final byte[] COLUMN_FAMILY_BYTES = "F".getBytes();
	private String schemaSuffix = null;
	private boolean onlyTriples = false;
	
	private Logger logger = Logger.getLogger("CoprocessorLog");
	
	public static final String []TABLE_NAMES = {"SPOC", "POCS", "OSPC", "OCSP", "CSPO", "CPSO"};
	public static final int SPOC = 0;
	public static final int POCS = 1;
	public static final int OSPC = 2;
	public static final int OCSP = 3;
	public static final int CSPO = 4;
	public static final int CPSO = 5;
	
	public static final int [][]OFFSETS = {{0,8,16,25}, {25,0,8,17}, {9,17,0,25}, {17,25,0,9}, {8,16,24,0}, {16,8,24,0}};
	
	private HTable []tables = null;
	 
	public static final String CONFIG_FILE_PATH = "hdfs://fs0:8020/user/sfu200/config.properties";//TODO check 
	public static final String COUNT_PROP = "COUNT";
	public static final String SUFFIX_PROP = "SUFFIX";
	public static final String ONLY_TRIPLES_PROP = "ONLY_TRIPLES";
	
	/**
	 * For testing purposes: count all Put operations when the coprocessor functions
	 * are called from multiple threads
	 */
	public AtomicInteger putCounter = new AtomicInteger();
	private volatile boolean keepGoing; 
	private volatile int state = INIT_STOPPED;
	private Thread mainThread;
	public static final int INIT_STOPPED = 0;
	public static final int RUNNING = 1;
	public static final int STOPPING = 2;
	private static final int ROW_KEY_SIZE = 33;
	private byte [] destRowKeys;
	
	public PrefixMatchGenerateSecondaryIndex() {
		super();
	}

	/**
	 * For testing purposes
	 */
	public PrefixMatchGenerateSecondaryIndex(String schemaSuffix, boolean onlyTriples, HTable[] tables) {
		super();
		this.schemaSuffix = schemaSuffix;
		this.onlyTriples = onlyTriples;
		this.tables = tables;
	}

	@Override
	public void start(CoprocessorEnvironment env) {
		super.start(env);
		
	}

	@Override
	public void stop(CoprocessorEnvironment env) {
		try {
			stopGeneration();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void stopGeneration() throws IOException {
		logger.info("["+Thread.currentThread().getName()+"]: Coprocessor stopping "+this+" from stopGeneration call");
		synchronized (this) {
			if (state == RUNNING){
				state = STOPPING;
				try {
					wait(10000);//wait 5 seconds
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				if (state != INIT_STOPPED){//the other thread did not finish gracefully so we interrupt
					mainThread.interrupt();
					try {
						Thread.sleep(2000);//wait 1 second for the thread to actually get interrupted
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		}
		state = INIT_STOPPED;
		logger.info("["+Thread.currentThread().getName()+"]: Finished waiting for main loop, now closing");
	}

	@Override
	public void generateSecondaryIndex() throws IOException{
		logger.info("["+Thread.currentThread().getName()+"]: Generate secondary index "+this);
		synchronized (this) {
			mainThread = Thread.currentThread();
			if (state == INIT_STOPPED)
				state = RUNNING;
			else{
				logger.info("Not running because not in INIT_STOPPED state");
				return;
			}
		}
		
		RegionCoprocessorEnvironment env = (RegionCoprocessorEnvironment)getEnvironment();
		 
		try {
			initSecondaryIndexTableConnections(env);
			logger.info("["+Thread.currentThread().getName()+"]: Coprocessor started "+this);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		Scan scan = new Scan();
	    scan.addFamily(COLUMN_FAMILY_BYTES);	   
	    InternalScanner scanner = env.getRegion().getScanner(scan);
	    
	    runMainLoop(scanner);
	    
		try {
			for (int i = 0; i < tables.length; i++) {
				tables[i].close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	    
		synchronized (this) {
			notify();
			state = INIT_STOPPED;
		}
		logger.info("["+Thread.currentThread().getName()+"]: Notified");
		
	}

	private void runMainLoop(InternalScanner scanner) throws IOException {
		List<KeyValue> currentKVList = new ArrayList<KeyValue>();
		destRowKeys = new byte[ROW_KEY_SIZE];
		try {
			do {
				currentKVList.clear();
				keepGoing = scanner.next(currentKVList);
				KeyValue onlyOne = currentKVList.get(0);
				//logger.info("[" + Thread.currentThread().getName() + "]: Processing new element");

				for (int i = 1; i <= tables.length; i++) {
					Put newPut = build(OFFSETS[i][0], OFFSETS[i][1], OFFSETS[i][2], OFFSETS[i][3], onlyOne);
					tables[i-1].put(newPut);
				}
			} while (keepGoing && (state == RUNNING));
		}
	    finally{
	    	scanner.close();
	    }
	}

	final private Put build(int sOffset, int pOffset, int oOffset, int cOffset, KeyValue kv) 
	{
		byte []backingBuffer = kv.getBuffer();
		int rowOffset = kv.getRowOffset();
		Bytes.putBytes(destRowKeys, sOffset, backingBuffer, rowOffset, 8);//put S 
		Bytes.putBytes(destRowKeys, pOffset, backingBuffer, rowOffset+8, 8);//put P 
		Bytes.putBytes(destRowKeys, oOffset, backingBuffer, rowOffset+16, 9);//put O
		Bytes.putBytes(destRowKeys, cOffset, backingBuffer, rowOffset+25, 8);//put C
		
		Put newPut = new Put(destRowKeys);
		newPut.add(COLUMN_FAMILY_BYTES, null, null);
		return newPut;
	}
	
	public static String hexaString(byte []b, int offset, int length){
		String ret = "";
		for (int i = offset; i < length; i++) {
			ret += String.format("\\x%02x", b[i]);
		}
		return ret;
	}
	
	private void initSecondaryIndexTableConnections(RegionCoprocessorEnvironment env) throws IOException {
		FSDataInputStream in = getConfigFile(env.getConfiguration());
		
		Properties prop = new Properties();
		prop.load(in);
		schemaSuffix = prop.getProperty(SUFFIX_PROP, "");
		onlyTriples = Boolean.parseBoolean(prop.getProperty(ONLY_TRIPLES_PROP, ""));	
		int tablesNumber = 6;
		if (onlyTriples == true){
			tablesNumber = 3;
		}
		logger.info("Tables number: "+tablesNumber);
		in.close();
		
		tables = new HTable[tablesNumber-1];//all tables except SPOC
		for (int i = 0; i < tables.length; i++) {
			tables[i] = new HTable(env.getConfiguration(), (TABLE_NAMES[i+1]+schemaSuffix).getBytes());
			tables[i].setAutoFlush(false);
			tables[i].setWriteBufferSize(40*1024*1024);
			tables[i].prewarmRegionCache(tables[i].getRegionsInfo());
		}
	}

	private FSDataInputStream getConfigFile(Configuration conf) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		Path configFile = new Path(CONFIG_FILE_PATH);
		
		if (!fs.exists(configFile)){
			throw new IOException("Configuration file not found");
		}
		if (!fs.isFile(configFile)){
			throw new IOException("Input path is not a file");
		}
			
		FSDataInputStream in = fs.open(configFile);
		return in;
	}
	
	
}
