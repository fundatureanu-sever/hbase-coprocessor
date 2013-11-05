package nl.vu.datalayer.coprocessor.joins;

import org.apache.hadoop.hbase.util.Bytes;

/**ASSUMPTION: expecting the input qualifier bytes to be
 * - 2 bytes - join id
 * - 1 byte - triple id
 * - 1 byte - encoding of join positions
 * - 1 byte id - for each variable id in a non-join position (in SPOC order)
 * example: S ?p ?o - join by ?o
 *			S - startRowkey
 *			inputQualifier
 *			tripleIdByte - e.g. 0x01
 *			joinByte - 0x02 (bit encoding of object position)
 *			id(?p) - 1 byte id which will be used in the join table
 */
class MetaInfo{
	private static final int TRIPLE_SIZE = 3;
	
	private short joinId;
	private byte tripleId;
	private byte joinPosition;
	private byte[] variableIds;
	private byte startRowKeyPositions;
	private byte startRowNumberOfElems;
	private int joinKeyLength;
	private int joinNumberOfElems;

	//private byte objectType;

	public MetaInfo(byte[] inputQualifier, byte[] startRow, int tableIndex) {
		super();
		this.joinId = Bytes.toShort(inputQualifier);
		this.tripleId = inputQualifier[2];
		this.joinPosition = inputQualifier[3];
		this.variableIds = Bytes.tail(inputQualifier, inputQualifier.length-4);
		startRowNumberOfElems = (byte)(startRow.length/JoinObserver.BASE_ID_SIZE);
		startRowKeyPositions = JoinObserver.KEY_ENCODINGS[tableIndex][startRowNumberOfElems];
		
		joinNumberOfElems = bitCount(joinPosition);
		joinKeyLength = joinNumberOfElems*JoinObserver.BASE_ID_SIZE;			
	}
	
	private int bitCount(byte b){
		int count = 0;
		for (int i = 0; i < 4; i++) {
			if ( (b & 1) == 1){
				count++;
			}
			b = (byte)(b>>1);
		}
		
		return count;
	}
	
	public boolean checkLengthParamters(){
		return (joinNumberOfElems+startRowNumberOfElems+variableIds.length==TRIPLE_SIZE);
	}
	
	public short getJoinId() {
		return joinId;
	}

	public byte getTripleId() {
		return tripleId;
	}

	public byte getJoinPosition(){
		return joinPosition;
	}
	
	public byte[] getVariableIds(){
		return variableIds;
	}
	
	public byte getStartRowKeyPositions(){
		return startRowKeyPositions;
	}
	
	public int getJoinKeyLength() {
		return joinKeyLength;
	}
	
	public void adjustJoinKeyLength(byte objectType){
		if ((joinPosition & (byte) JoinObserver.O) == JoinObserver.O) {			
			if (objectType == JoinObserver.NUMERICAL_TYPE) {//join on object
				joinKeyLength++;
			}
		}	
	}
	
}