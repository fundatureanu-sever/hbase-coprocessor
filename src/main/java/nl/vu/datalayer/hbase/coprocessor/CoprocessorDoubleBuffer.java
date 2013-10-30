package nl.vu.datalayer.hbase.coprocessor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.hbase.client.Put;

public class CoprocessorDoubleBuffer {
	public static final int BUFFER_NO = 2;
	private ArrayList<List<Put>> putBuffers;
	
	private ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();//protects reading from current buffer
																//vs swapping the 2 buffers
	
	private volatile int currentBufferIndex;

	public CoprocessorDoubleBuffer() {
		super();
		putBuffers = new ArrayList<List<Put>>();
		for (int i = 0; i < BUFFER_NO; i++) {
			putBuffers.add(Collections.synchronizedList(new ArrayList<Put>()));
		}
		
		currentBufferIndex = 0;
	}
	
	public void addPutToCurrentBuffer(Put newPut){
		rwl.readLock().lock();
		putBuffers.get(currentBufferIndex).add(newPut);
		rwl.readLock().unlock();
	}

	/*public List<Put> getCurrentBuffer() {
		return putBuffers.get(currentBufferIndex);
	}*/
	
	public int getCurrentBufferSize(){
		//rwl.readLock().lock();
		return putBuffers.get(currentBufferIndex).size();
		//rwl.readLock().unlock();
	}
	
	public void switchBuffers(){
		rwl.writeLock().lock();
		currentBufferIndex = (currentBufferIndex+1)%BUFFER_NO;
		rwl.writeLock().unlock();
	}
	
	public List<Put> getNextBuffer(){
		int index = (currentBufferIndex+1)%BUFFER_NO;
		return putBuffers.get(index);
	}
	
}
