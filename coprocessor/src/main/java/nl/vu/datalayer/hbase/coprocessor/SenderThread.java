package nl.vu.datalayer.hbase.coprocessor;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

public class SenderThread extends Thread {
	
	private Logger senderLogger = Logger.getLogger("SenderThread");
	
	public static final int [][]OFFSETS = {{0,8,16,25}, {25,0,8,17}, {9,17,0,25}, {17,25,0,9}, {8,16,24,0}, {16,8,24,0}};

	private LinkedBlockingQueue<Put> sharedQueue = new LinkedBlockingQueue<Put>();
	
	private HTableInterface []tables = null;
	
	private boolean keepGoing = true;
	
	public SenderThread(LinkedBlockingQueue<Put> sharedQueue, HTableInterface[] tables) {
		super();
		this.sharedQueue = sharedQueue;
		this.tables = tables;
	}
	
	public void pleaseStop(){
		keepGoing = false;
	}

	@Override
	public void run() {

		try {
			senderLogger.info("SenderThread: Ready to rumble");
			while (true) {
				
				Put put = sharedQueue.poll(10, TimeUnit.SECONDS);
				if (keepGoing == false)//stop gracefully
					break;
				
				if (put == null)//just a timeout
					continue;
				
				for (int i = 1; i <= tables.length; i++) {
					Put newPut = build(OFFSETS[i][0], OFFSETS[i][1], OFFSETS[i][2], OFFSETS[i][3], put.getRow());
					tables[i-1].put(newPut);
				}

			}
		} catch (InterruptedException e) {
			System.err.println("Finishing sender thread");
			e.printStackTrace();
		} catch (IOException e) {
			System.err.println("Finishing sender thread");
			e.printStackTrace();
		}
	}

	final static Put build(int sOffset, int pOffset, int oOffset, int cOffset, byte []source) 
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

}
