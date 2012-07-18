package nl.vu.datalayer.hbase.coprocessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.WritableByteArrayComparable;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.log4j.Logger;

public class PrefixMatchSecondaryIndex extends BaseRegionObserver {

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
	
	private HTable []tables = null;
	
	private LinkedBlockingQueue<Put> sharedQueue = new LinkedBlockingQueue<Put>();
	 
	public static final String CONFIG_FILE_PATH = "hdfs://fs0.cm.cluster:8020/user/sfu200/config.properties";//TODO check 
	public static final String COUNT_PROP = "COUNT";
	public static final String SUFFIX_PROP = "SUFFIX";
	public static final String ONLY_TRIPLES_PROP = "ONLY_TRIPLES";
	
	/**
	 * For testing purposes: count all Put operations when the coprocessor functions
	 * are called from multiple threads
	 */
	public AtomicInteger putCounter = new AtomicInteger(); 
	
	private SenderThread senderThread = null;
	
	public PrefixMatchSecondaryIndex() {
		super();
	}

	/**
	 * For testing purposes
	 */
	public PrefixMatchSecondaryIndex(String schemaSuffix, boolean onlyTriples, HTable[] tables) {
		super();
		this.schemaSuffix = schemaSuffix;
		this.onlyTriples = onlyTriples;
		this.tables = tables;
		senderThread = new SenderThread(sharedQueue, tables);
		senderThread.start();
		logger.info("SenderThread started");
	}

	@Override
	public void start(CoprocessorEnvironment e) throws IOException {		
		initSecondaryIndexTableConnections(e.getConfiguration());
		senderThread = new SenderThread(sharedQueue, tables);
		senderThread.start();
		logger.info("SenderThread started");
	}

	@Override
	public void stop(CoprocessorEnvironment e) throws IOException {
		senderThread.pleaseStop();
		logger.info("Requested sender thread to finish, now waiting ..");
		try {
			senderThread.join();
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		logger.info("Sender thread finished");
		if (tables != null){
			for (int i = 0; i < tables.length; i++) {
				tables[i].close();
			}
		}
	}

	public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, boolean writeToWAL) throws IOException {		
		sharedQueue.add(put);
	}	

	private void initSecondaryIndexTableConnections(Configuration hbaseConf) throws IOException {
		FSDataInputStream in = getConfigFile(hbaseConf);
		
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
			tables[i] = new HTable(hbaseConf, (TABLE_NAMES[i+1]+schemaSuffix).getBytes());
			tables[i].setAutoFlush(false);//TODO check if this would induce deadlock
		}
	}

	private FSDataInputStream getConfigFile(Configuration conf) throws IOException {
		//Configuration conf = new Configuration());
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
