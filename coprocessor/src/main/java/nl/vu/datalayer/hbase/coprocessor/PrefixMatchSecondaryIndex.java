package nl.vu.datalayer.hbase.coprocessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.WritableByteArrayComparable;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.SimpleLayout;

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
	
	public static final int [][]OFFSETS = {{0,8,16,25}, {25,0,8,17}, {9,17,0,25}, {17,25,0,9}, {8,16,24,0}, {16,8,24,0}};
	
	private HTableInterface []tables = null;
	
	/**
	 * Buffer for put operations - we need to do it ourselves as we can't set
	 * autoFlush to false from the HTableInterface
	 */
	private ArrayList<ArrayList<Put>> batchPuts = new ArrayList<ArrayList<Put>>();
	private int tablesNumber = 6;
	
	public static final int FLUSH_LIMIT = 5;//TODO set to an appropriate value
	 
	
	public static final String CONFIG_FILE_PATH = "file:///var/scratch/sfu200/config.properties";//TODO check 
	public static final String COUNT_PROP = "COUNT";
	public static final String SUFFIX_PROP = "SUFFIX";
	public static final String ONLY_TRIPLES_PROP = "ONLY_TRIPLES";
	
	private ArrayList<SchemaInfo> schemas = new ArrayList<SchemaInfo>();
	
	@Override
	public void start(CoprocessorEnvironment e) throws IOException {
		super.start(e);

		initSchemaInfoList();
	}

	@Override
	public void stop(CoprocessorEnvironment e) throws IOException {
		super.stop(e);
		if (tables != null){
			for (int i = 0; i < tables.length; i++) {
				tables[i].close();
			}
		}
	}

	@Override
	public boolean preCheckAndPut(ObserverContext<RegionCoprocessorEnvironment> e, byte[] row, byte[] family, byte[] qualifier, CompareOp compareOp, WritableByteArrayComparable comparator, Put put, boolean result) throws IOException {
		logger.info("Row: "+(row==null? null : row.length)+
				"; Family: "+(family==null?null:family.length)+
				"; Qualifier: "+(qualifier==null?null:qualifier.length));
		if (qualifier!=null && qualifier.length==1 && qualifier[0]=='c'){//we consider it is a flush marker
			if (tables!=null){
				flushBatchPuts();
			}
			e.bypass();
			return false;
		}
		return true;
	}

	public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, boolean writeToWAL) throws IOException {
		if (schemaSuffix == null){
			init(e);
		}
		
		for (int i = 1; i < tablesNumber; i++) {
			Put newPut = build(OFFSETS[i][0], OFFSETS[i][1], OFFSETS[i][2], OFFSETS[i][3], put.getRow());
			batchPuts.get(i-1).add(newPut);
		}
		logger.info("New put detected: "+batchPuts.get(0).size());
		
		if (batchPuts.get(0).size() == FLUSH_LIMIT){
			flushBatchPuts();
		}
	}

	private void flushBatchPuts() throws IOException {
		for (int i=0; i<tables.length; i++) {
			tables[i].put(batchPuts.get(i));
			batchPuts.get(i).clear();
		}
		
		logger.info("New batch of "+FLUSH_LIMIT+" puts issued");
	}

	final private void init(ObserverContext<RegionCoprocessorEnvironment> e) throws IOException {
		
		String tableName = e.getEnvironment().getRegion().getRegionInfo().getTableNameAsString();
		schemaSuffix = tableName.substring(TABLE_NAMES[SPOC].length());
		logger.info("Current schema suffix: "+schemaSuffix);
		
		for (SchemaInfo si : schemas) {
			if (si.suffix.equals(schemaSuffix)){
				onlyTriples = si.onlyTriples;
				break;
			}
		}
		if (onlyTriples == true){
			tablesNumber = 3;
		}
		logger.info("Tables number: "+tablesNumber);
		
		tables = new HTableInterface[tablesNumber-1];//all tables without SPOC
		for (int i = 0; i < tables.length; i++) {
			batchPuts.add(new ArrayList<Put>());
			tables[i] = e.getEnvironment().getTable((TABLE_NAMES[i+1]+schemaSuffix).getBytes());
		}
		logger.info("Finished initializing tables");
	}

	private void initSchemaInfoList() throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path configFile = new Path(CONFIG_FILE_PATH);
		Properties prop = new Properties();
		
		if (!fs.exists(configFile)){
			throw new IOException("Configuration file not found");
		}
		if (!fs.isFile(configFile)){
			throw new IOException("Input path is not a file");
		}
			
		FSDataInputStream in = fs.open(configFile);
		prop.load(in);
		String numberOfSchemasStr = prop.getProperty(COUNT_PROP, "");
		int numberOfSchemas = Integer.parseInt(numberOfSchemasStr);
		for (int i = 0; i < numberOfSchemas; i++) {
			String suffix = prop.getProperty(SUFFIX_PROP, "");
			boolean onlyTriples = Boolean.parseBoolean(prop.getProperty(ONLY_TRIPLES_PROP, ""));
			schemas.add(new SchemaInfo(suffix, onlyTriples));
			logger.info("New schema added: "+suffix+" "+onlyTriples);
		}
		
		in.close();
	}
	
	final private static Put build(int sOffset, int pOffset, int oOffset, int cOffset, byte []source) 
	{
		byte []key = new byte[source.length];
		Bytes.putBytes(key, sOffset, source, 0, 8);//put S 
		Bytes.putBytes(key, pOffset, source, 8, 8);//put P 
		Bytes.putBytes(key, oOffset, source, 16, 9);//put O
		Bytes.putBytes(key, cOffset, source, 25, 8);//put C
		
		Put newPut = new Put(key);
		newPut.add("F".getBytes(), null, null);
		
		return newPut;
	}

	/*@Override
	public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, boolean writeToWAL) throws IOException {
		HTableInterface table = e.getEnvironment().getTable("test2".getBytes());
		byte []key = put.getRow(); //get the key in SPOC order
		
		byte []newKey = new byte[HBPrefixMatchSchema.KEY_LENGTH];
		System.arraycopy(key, 8, newKey, 0, newKey.length-8);
		System.arraycopy(key, 0, newKey, newKey.length-8, 8);
		
		Put newPut = new Put(newKey);
		newPut.add(HBPrefixMatchSchema.COLUMN_FAMILY, HBPrefixMatchSchema.COLUMN_NAME, "".getBytes());
		table.put(newPut);
	}*/

	private class SchemaInfo{
		private String suffix;
		private boolean onlyTriples;
		
		public SchemaInfo(String suffix, boolean onlyTriples) {
			super();
			this.suffix = suffix;
			this.onlyTriples = onlyTriples;
		}
	}
}
