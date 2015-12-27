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
	private static final int MSG_PADDING_LENGTH = 2 + 1;// 2 bytes lenth and 1
														// byte flag

	private final UnsafeMemory mm;
	private final QueueMeta metaData;

	public long getStartPos() {
		return 8 * 2 + 1;
	}

	public long getEndPos() {
		return mm.getSize();
	}

	public SharedMMRing(QueueMeta metaData, long rawMemoryStartAddr) {
		super();
		this.mm = new UnsafeMemory(metaData.getAddr() + rawMemoryStartAddr, metaData.getRawLenth());
		this.metaData = metaData;
		// next data start addr
		if (mm.compareAndSwapLong(0, 0, 8 + 8 + 1)) {
			// cur write begin addr
			mm.putLongVolatile(8, 8 + 8 + 2);
			// data commited and none next flag
			mm.putByteVolatile(8 + 8, FLAG_NO_NEXT);
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

	private boolean tryReWindWrite(byte[] rawMsg) {
		int dataRealLen = getMsgTotalSpace(rawMsg);
		long writeStartPos = this.getWriteStartAddr();
		long nextDataPos = this.getNextDataAddr();
		// System.out.println("rewind write ,cur write addr :" + writeStartPos +
		// " nextDataPos " + nextDataPos
		// + " data len:" + dataRealLen);

		// enough space to write
		if (writeStartPos + dataRealLen < nextDataPos) {
			if (!mm.compareAndSwapLong(8, writeStartPos, writeStartPos + dataRealLen)) {
				return false;
			}
			// write data
			writeMsg(writeStartPos, rawMsg);
			if (writeStartPos == this.getStartPos() + 1) {// first rewind write
															// update prev
															// data's next flag
															// to rewind
				mm.putByteVolatile(nextDataPos, MASK_NEXT_REWIND);
			}

			return true;
		} else {
			throw new java.lang.RuntimeException("no space to write ");
		}
	}

	private boolean tryWriteData(byte[] rawMsg) {

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
					return false;
				}
				// write data
				this.writeMsg(writeStartPos, rawMsg);
				// update prev data's next flag
				mm.putByteVolatile(writeStartPos - 1, FLAG_NEXT_ADJACENT);
				return true;
			} else {
				System.out.println("rewind write start Pos " + writeStartPos);
				// try rewind write
				// first set writeStartPos to start of queue
				mm.compareAndSwapLong(8, writeStartPos, this.getStartPos() + 1);
				return tryReWindWrite(rawMsg);
			}

		} else {// rewind from begin ,try wrap write
			System.out.println("write rewindw start Pos " + writeStartPos);
			return tryReWindWrite(rawMsg);
		}

	}

	public void addData(byte[] rawMsg) {
		boolean success = false;
		for (int i = 0; i < 2; i++) {
			if (success = tryWriteData(rawMsg)) {
				break;
			}
		}
		if (!success) {
			throw new RuntimeException("can't addData,not enough memery");
		}
	}

	public byte[] pullData() {
		byte[] msg = null;
		long dataStartPos = this.getNextDataAddr();
		byte nextDataFlag = mm.getByteVolatile(dataStartPos);
		switch (nextDataFlag) {
		case FLAG_NO_NEXT: {
			break;
		}
		case FLAG_NEXT_ADJACENT: {
			int dataLength = mm.getShort(dataStartPos + 1);
			msg = new byte[dataLength];
			long nextDataStartAddr = dataStartPos + MSG_PADDING_LENGTH;
			mm.getBytes(nextDataStartAddr, msg, 0, dataLength);
			mm.compareAndSwapLong(0, dataStartPos, nextDataStartAddr + dataLength);
			break;
		}
		case MASK_NEXT_REWIND: {
			long newDataStartPos = this.getStartPos();
			int dataLength = mm.getShort(newDataStartPos + 1);
			msg = new byte[dataLength];
			long nextDataStartAddr = newDataStartPos + MSG_PADDING_LENGTH;
			mm.getBytes(nextDataStartAddr, msg, 0, dataLength);
			mm.compareAndSwapLong(0, dataStartPos, nextDataStartAddr + dataLength);
			break;
		}

		}
		return msg;
	}
}
