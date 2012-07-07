package nl.vu.datalayer.hbase.coprocessor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.client.Put;

public class CoprocessorDoubleBuffer {
	public static final int BUFFER_NO = 2;
	private ArrayList<List<Put>> putBuffers;
	
	private volatile int currentBufferIndex;

	public CoprocessorDoubleBuffer() {
		super();
		putBuffers = new ArrayList<List<Put>>();
		for (int i = 0; i < BUFFER_NO; i++) {
			putBuffers.add(Collections.synchronizedList(new ArrayList<Put>()));
		}
		
		currentBufferIndex = 0;
	}

	public List<Put> getCurrentBuffer() {
		return putBuffers.get(currentBufferIndex);
	}
	
	public int currentBufferSize(){
		return putBuffers.get(currentBufferIndex).size();
	}
	
	public void switchBuffers(){
		//int temp = 
		currentBufferIndex = (currentBufferIndex+1)%BUFFER_NO;
	}
	
	public List<Put> getNextBuffer(){
		int index = (currentBufferIndex+1)%BUFFER_NO;
		return putBuffers.get(index);
	}
	
}
