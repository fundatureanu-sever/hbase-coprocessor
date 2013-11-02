package nl.vu.datalayer.coprocessor.schema;

public class PrefixMatchSchema {

	public static final String []TABLE_NAMES = {"SPOC", "POCS", "OSPC", "OCSP", "CSPO", "CPSO"};
	public static final byte[] COLUMN_FAMILY_BYTES = "F".getBytes();
	public static final byte[] COLUMN_BYTES = "".getBytes();
	public static final int SPOC = 0;
	public static final int POCS = 1;
	public static final int OSPC = 2;
	public static final int OCSP = 3;
	public static final int CSPO = 4;
	public static final int CPSO = 5;

}
