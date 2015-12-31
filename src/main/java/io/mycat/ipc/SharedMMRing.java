package io.mycat.ipc;

/**
 * SharedMemory Ring ,Allow multi writers and readers long dataStartAddr long
 * dataEndAddr
 * 
 * @author wuzhih
 * 
 */
public class SharedMMRing {
	// means this message notewrited, writed ,readed
	private static final byte FLAG_NO_NEXT = 0B0001;
	private static final byte FLAG_NEXT_ADJACENT = 0B0010;
	private static final byte MASK_NEXT_REWIND = 0B0100;
	private static final int MSG_PADDING_LENGTH = 2 + 1;// 2 bytes lenth and
														// flag

	private final UnsafeMemory mm;
	private final QueueMeta metaData;

	public long getStartPos() {
		return 8 * 2;
	}

	public long getEndPos() {
		return mm.getSize();
	}

	public SharedMMRing(QueueMeta metaData, long rawMemoryStartAddr) {
		super();
		this.mm = new UnsafeMemory(metaData.getAddr() + rawMemoryStartAddr, metaData.getRawLenth());
		this.metaData = metaData;
		// next data start addr
		if (mm.compareAndSwapLong(0, 0, 8 + 8)) {
			// no next data flag
			mm.putByteVolatile(8 + 8, FLAG_NO_NEXT);
			// cur write begin addr
			mm.putLongVolatile(8, 8 + 8 + 1);

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

	private int tryReWindWrite(byte[] rawMsg, long lastWriteStartPos) {
		int dataRealLen = getMsgTotalSpace(rawMsg);
		long writeStartPos = this.getWriteStartAddr();
		long nextDataPos = this.getNextDataAddr();
		// System.out.println("rewind write ,cur write addr :" + writeStartPos +
		// " nextDataPos " + nextDataPos
		// + " data len:" + dataRealLen);

		// enough space to write
		if (writeStartPos + dataRealLen < nextDataPos) {
			if (!mm.compareAndSwapLong(8, writeStartPos, writeStartPos + dataRealLen)) {
				return 1;
			}
			// write data
			writeMsg(writeStartPos, rawMsg);
			if (writeStartPos == this.getStartPos()) {// first rewind write
														// update prev
														// data's next flag
														// to rewind
				//mm.putByteVolatile(nextDataPos, MASK_NEXT_REWIND);
				//fixed reader pending bug
				mm.putByteVolatile(lastWriteStartPos - 1, MASK_NEXT_REWIND);
			} else {
				// update prev data's next flag
				mm.putByteVolatile(writeStartPos - 1, FLAG_NEXT_ADJACENT);
			}

			return 0;
		} else if ((writeStartPos > nextDataPos)) {
			System.out.println(
					"warn xxxxxxxxx " + writeStartPos + " nextDataPos " + nextDataPos + " data len:" + dataRealLen);
			return 1;
		} else {

			System.out.println("no more space cur write addr :" + writeStartPos + " nextDataPos " + nextDataPos
					+ " data len:" + dataRealLen);
			 throw new java.lang.RuntimeException("no space to write ");
			//return -1;
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
				if (!mm.compareAndSwapLong(8, writeStartPos, writeStartPos + dataRealLen)) {
					return 1;
				}
				// write data
				this.writeMsg(writeStartPos, rawMsg);
				// update prev data's next flag
				mm.putByteVolatile(writeStartPos - 1, FLAG_NEXT_ADJACENT);
				return 0;
			} else {
				System.out.println("rewind write ,before, start Pos " + writeStartPos + " next " + nextDataBeginPos);
				// try rewind write
				// first set writeStartPos to start of queue
				mm.compareAndSwapLong(8, writeStartPos, this.getStartPos());
				System.out.println("rewind write ,after, start Pos " + this.getWriteStartAddr() + " next " + this.getNextDataAddr());
				//last writeStartPos
				return tryReWindWrite(rawMsg, writeStartPos);
			}

		} else {// rewind from begin ,try wrap write
			// System.out.println("write rewindw start Pos " + writeStartPos);
			//last writeStartPos
			return tryReWindWrite(rawMsg, writeStartPos);
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

	public byte[] pullData() {
		byte[] msg = null;
		long nextDataFlagPos = this.getNextDataAddr();
		byte nextDataFlag = mm.getByteVolatile(nextDataFlagPos);
		switch (nextDataFlag) {
		case FLAG_NO_NEXT: {
//			System.out.println(
//					"no data to  read ,data next pos " + nextDataFlagPos + ",  write pos " + this.getWriteStartAddr());
			break;
		}
		case FLAG_NEXT_ADJACENT: {
			int dataLength = mm.getShort(nextDataFlagPos + 1);
			msg = new byte[dataLength];
			long nextDataStartAddr = nextDataFlagPos + MSG_PADDING_LENGTH;
			mm.getBytes(nextDataStartAddr, msg, 0, dataLength);
			mm.compareAndSwapLong(0, nextDataFlagPos, nextDataStartAddr + dataLength);
			break;
		}
		case MASK_NEXT_REWIND: {
			System.out.println("rewind read begin,data next pos " + nextDataFlagPos);
			long newDataStartPos = this.getStartPos();
			int dataLength = mm.getShort(newDataStartPos);
			msg = new byte[dataLength];
			mm.getBytes(newDataStartPos + 2, msg, 0, dataLength);
			// prev data flag byte
			long nextDataStartAddr = newDataStartPos + MSG_PADDING_LENGTH - 1 + dataLength;
			mm.compareAndSwapLong(0, nextDataFlagPos, nextDataStartAddr);
			System.out.println("rewind read ,data next pos " + nextDataStartAddr + ", data len: " + dataLength
					+ " write pos " + this.getWriteStartAddr());
			break;
		}

		}
		return msg;
	}
}
