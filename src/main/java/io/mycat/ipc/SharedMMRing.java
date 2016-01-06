package io.mycat.ipc;

/**
 * SharedMemory Ring ,Allow multi writers and readers long dataStartAddr long
 * dataEndAddr
 * 
 * @author wuzhih
 * 
 */
public class SharedMMRing {
	public static final byte STORAGE_PRIMARY=0;
	public static final byte STORAGE_EXTEND=1;
	public static final byte STORAGE_EXT_FILE=2;
	// means next data condition
	public static final byte FLAG_NO_NEXT = 0B0001;
	public static final byte FLAG_NEXT_ADJACENT = 0B0010;
	public static final byte MASK_NEXT_REWIND = 0B0100;
	public static final byte MASK_NEXT_EXTEND_RING = 0B0101;
	public static final byte MASK_NEXT_BLOCK = 0B0110;
	public static final int MSG_PADDING_LENGTH = 2 + 1;// 2 bytes lenth and
														// flag

	private final UnsafeMemory mm;
	private final QueueMeta metaData;
	
	public final long getStartPos() {
		return 8 * 2;
	}

	public final long getWriteIndexPos() {
		return 8;
	}

	public final long getNextDataFlagIndexPos() {
		return 0;
	}

	public final long getEndPos() {
		return mm.getSize();
	}

	public SharedMMRing(QueueMeta metaData, long rawMemoryStartAddr) {
		super();
		this.mm = new UnsafeMemory(metaData.getAddr() + rawMemoryStartAddr,rawMemoryStartAddr, metaData.getRawLenth());
		this.metaData = metaData;
		// next data start addr
		if (mm.compareAndSwapLong(getNextDataFlagIndexPos(), 0, this.getStartPos())) {

			// cur write begin addr
			mm.putLongVolatile(getWriteIndexPos(),this.getStartPos()+1);
			// no next data flag
			mm.putByte(getStartPos(), FLAG_NO_NEXT);

		}

	}

	private void writeMsg(long pos, byte[] data) {
		short msgLength = (short) data.length;
		mm.putShort(pos, msgLength);
		mm.setBytes(pos + 2, data, 0, msgLength);
		mm.putByte(pos + 2 + msgLength, FLAG_NO_NEXT);

	}

	private int getMsgTotalSpace(byte[] msg) {
		return msg.length + MSG_PADDING_LENGTH;
	}

	public long getNextDataAddr() {
		return mm.getLongVolatile(0);

	}

	public long getWriteStartAddr() {
		return mm.getLongVolatile(8);

	}

	public UnsafeMemory getMm() {
		return mm;
	}

	public QueueMeta getMetaData() {
		return metaData;
	}

	private int tryReWindWrite(byte[] rawMsg) {
		int dataRealLen = getMsgTotalSpace(rawMsg);
		long writeStartPos = this.getWriteStartAddr();
		long nextDataPos = this.getNextDataAddr();
		// System.out.println("rewind write ,cur write addr :" + writeStartPos +
		// " nextDataPos " + nextDataPos
		// + " data len:" + dataRealLen);

		// enough space to write
		if (writeStartPos + dataRealLen < nextDataPos) {
			if (!mm.compareAndSwapLong(getWriteIndexPos(), writeStartPos, writeStartPos + dataRealLen)) {
				return 1;
			}
			// write data
			writeMsg(writeStartPos, rawMsg);
			// update prev data's next flag
			mm.putByteVolatile(writeStartPos - 1, FLAG_NEXT_ADJACENT);
			return 0;
		} else if ((writeStartPos > nextDataPos)) {
			return 1;
		} else {

			System.out.println("no more space cur write addr :" + writeStartPos + " nextDataPos " + nextDataPos
					+ " data len:" + dataRealLen);
			// throw new java.lang.RuntimeException("no space to write ");
			// return -1;
			return -1;
		}
	}

	/**
	 * return 0 ,wirte ok , 1 ,try agagin ,-1 no space
	 * 
	 * @param rawMsg
	 * @return
	 */

	private int tryWriteData(byte[] rawMsg) {

		long nextDataBeginPos = this.getNextDataAddr();
		long writeStartPos = getWriteStartAddr();
		// normal append write to tail
		if (writeStartPos > nextDataBeginPos) {
			int dataRealLen = getMsgTotalSpace(rawMsg);
			// enough space
			if (writeStartPos + dataRealLen <= this.getEndPos()) {
				// System.out.println("write start Pos " + writeStartPos);
				// first update writeStart pos
				if (!mm.compareAndSwapLong(getWriteIndexPos(), writeStartPos, writeStartPos + dataRealLen)) {
					return 1;
				}
				// write data
				this.writeMsg(writeStartPos, rawMsg);
				// update prev data's next flag
				mm.putByteVolatile(writeStartPos - 1, FLAG_NEXT_ADJACENT);
				return 0;
			} else {
				// System.out.println("rewind write ,before, start Pos " +
				// writeStartPos + " next " + nextDataBeginPos);
				// try rewind write

				// first set writeStartPos to start of queue
				if (mm.compareAndSwapLong(getWriteIndexPos(), writeStartPos, this.getStartPos() + 1)) {
					// set no more data flag
					mm.putByte(this.getStartPos(), FLAG_NO_NEXT);
					// set previous data's next flag to rewind
					mm.putByteVolatile(writeStartPos - 1, MASK_NEXT_REWIND);
				}
				// System.out.println("rewind write ,after, start Pos " +
				// this.getWriteStartAddr() + " next " +
				// this.getNextDataAddr());
				// last writeStartPos
				return tryReWindWrite(rawMsg);
			}

		} else {// rewind from begin ,try wrap write
			// System.out.println("write rewindw start Pos " + writeStartPos);
			// last writeStartPos
			return tryReWindWrite(rawMsg);
		}

	}

	/**
	 * if return false,means can't put ,ring is full
	 * 
	 * @param rawMsg
	 * @return
	 */
	public boolean putData(byte[] rawMsg) {

		for (int i = 0; i < 3; i++) {
			int writeResult = tryWriteData(rawMsg);
			switch (writeResult) {
			case 0:
				return true;
			case 1:
				continue;
			case -1:
				return false;

			}

		}
		return false;
	}

	private byte[] readData(long prevNextDataFlagPos, long curDataFlagPos) {
		int dataLength = mm.getShort(curDataFlagPos + 1);
		long nextDataStartAddr = curDataFlagPos + MSG_PADDING_LENGTH;
		if (!mm.compareAndSwapLong(getNextDataFlagIndexPos(), prevNextDataFlagPos, nextDataStartAddr + dataLength)) {
			return null;
		}
		byte[] msg = new byte[dataLength];
		mm.getBytes(nextDataStartAddr, msg, 0, dataLength);
		return msg;
	}

	public byte[] pullData() {
		byte[] msg = null;
		long nextDataFlagPos = this.getNextDataAddr();
		byte nextDataFlag = mm.getByteVolatile(nextDataFlagPos);
		switch (nextDataFlag) {
		case FLAG_NO_NEXT: {
			// System.out.println(
			// "no data to read ,data next pos " + nextDataFlagPos + ", write
			// pos " + this.getWriteStartAddr());
			break;
		}
		case FLAG_NEXT_ADJACENT: {
			msg = readData(nextDataFlagPos, nextDataFlagPos);
			break;
		}
		case MASK_NEXT_REWIND: {
			byte newNextDataFlag = mm.getByteVolatile(this.getStartPos());
			/*
			 * System.out.println("rewind read begin,data next pos " +
			 * nextDataFlagPos+ " next flag "+newNextDataFlag);
			 */
			switch (newNextDataFlag) {
			case FLAG_NO_NEXT: {
				mm.compareAndSwapLong(getNextDataFlagIndexPos(), nextDataFlagPos, this.getStartPos());
				break;
			}
			case FLAG_NEXT_ADJACENT: {
				msg = readData(nextDataFlagPos, this.getStartPos());
				break;
			}
			}
			// System.out.println("rewind read ,data next pos " +
			// nextDataStartAddr + ", data len: " + dataLength
			// + " write pos " + this.getWriteStartAddr());
		}

		}
		return msg;
	}
}
