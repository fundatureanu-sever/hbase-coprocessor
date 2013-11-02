package nl.vu.datalayer.coprocessor.schema;

public class PrefixMatchSchema {

	public static final String []TABLE_NAMES = {"SPOC", "POCS", "OSPC", "OCSP", "CSPO", "CPSO"};
	public static final int [][]OFFSETS = {{0,8,16,25}, {25,0,8,17}, {9,17,0,25}, {17,25,0,9}, {8,16,24,0}, {16,8,24,0}};
	
	public static final byte[] COLUMN_FAMILY_BYTES = "F".getBytes();
	public static final byte[] COLUMN_BYTES = "".getBytes();
	public static final int SPOC = 0;
	public static final int POCS = 1;
	public static final int OSPC = 2;
	public static final int OCSP = 3;
	public static final int CSPO = 4;
	public static final int CPSO = 5;
	
	public static final String TEMP_TABLE_NAME = "JOIN_TABLE";//TODO should be multiple tables
	

}
