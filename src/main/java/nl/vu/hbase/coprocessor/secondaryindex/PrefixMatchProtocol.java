package nl.vu.hbase.coprocessor.secondaryindex;

import java.io.IOException;

import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

public interface PrefixMatchProtocol extends CoprocessorProtocol {
	
	public void generateSecondaryIndex() throws IOException;
	
	public void stopGeneration() throws IOException;
}
