package io.mycat.ipc;

import java.io.File;

import io.mycat.ipc.util.Util;

/**
 * use memory mapping method to speed up large file write and read
 * 
 * @author wuzhih
 *
 */
public class FastestFileStorage {
	private static final int fileHeaderLen = 8 + 8;
	private final long mapBlockSize;
	private final String loc;
	private final boolean writeMode;
	private UnsafeMemory fileHeaderMM;
	private volatile UnsafeMemory curMappingMM;
	private final long maxFileLen;
	public static final int MSG_PADDING_LENGTH = 2 + 1;// 2 bytes lenth and
	// flag

	/**
	 * Constructs a new memory mapped file.
	 * 
	 * @param loc
	 *            the file name
	 * @param len
	 *            the file length
	 * @throws Exception
	 *             in case there was an error creating the memory mapped file
	 */
	public FastestFileStorage(final String loc, long maxLen, boolean writerMode, int mapBlockSize) throws Exception {
		if (writerMode) {
			new File(loc).delete();
		}
		this.writeMode = writerMode;
		this.loc = loc;
		this.maxFileLen = Util.roundTo4096(maxLen);
		this.mapBlockSize = Util.roundTo4096(mapBlockSize);
		this.fileHeaderMM = UnsafeMemory.mapAndSetOffset(loc, maxFileLen, writerMode, 0, fileHeaderLen);
		init();

	}

	public final long getNextDataFlagIndexPos() {
		return 0;
	}

	public final long getWriteIndexPos() {
		return 8;
	}

	public final long getStartPos() {
		return 8 * 2;
	}

	private void init() throws Exception {
		// next data start addr
		if (fileHeaderMM.compareAndSwapLong(getNextDataFlagIndexPos(), 0, this.getStartPos())) {
			// cur write begin addr
			fileHeaderMM.putLongVolatile(getWriteIndexPos(), this.getStartPos() + 1);
			// no next data flag
			fileHeaderMM.putByte(getStartPos(), SharedMMRing.FLAG_NO_NEXT);
		}
		if (writeMode) {
			curMappingMM = UnsafeMemory.mapAndSetOffset(loc, maxFileLen, this.writeMode, 0, this.mapBlockSize);
		}
	}

	public void close() throws Exception {
		fileHeaderMM.unmap();
		curMappingMM.unmap();
	}

	public boolean readerFarBehindWriter() {
		long nextDataBeginPos = this.getNextDataAddr();
		long writeStartPos = getWriteStartAddr();
		return (nextDataBeginPos == this.getStartPos() || (writeStartPos - nextDataBeginPos) * 2 > maxFileLen);
	}

	public int writeData(byte[] rawMsg) throws Exception {
		long writeStartPos = getWriteStartAddr();
		int dataRealLen = getMsgTotalSpace(rawMsg);
		// not exceed file lenth
		if (writeStartPos + dataRealLen < maxFileLen - fileHeaderLen) {
			long endPos = writeStartPos + dataRealLen;
			UnsafeMemory mm = this.curMappingMM;
			long dataRelativePos = 0;
			// have space in this map block
			if (mm.getEndPos() >= endPos) {
				if (!fileHeaderMM.compareAndSwapLong(getWriteIndexPos(), writeStartPos, endPos)) {
					return 0;
				}
				// write data
				dataRelativePos = mm.getRelovePos(writeStartPos);
				this.writeMsg(mm, dataRelativePos, rawMsg);
				
			} else {// next mapping block;
				// first set writeStartPos to next block start addr
				long nextWritePos = mm.getEndPos() + 1;
				if (fileHeaderMM.compareAndSwapLong(getWriteIndexPos(), writeStartPos, nextWritePos)) {
					curMappingMM = UnsafeMemory.mapAndSetOffset(loc, maxFileLen, false, mm.getEndPos(),
							this.mapBlockSize);
					UnsafeMemory nextmm = curMappingMM;
					// set no more data flag first for the new block
					nextmm.putByte(0, SharedMMRing.FLAG_NO_NEXT);
					// set previous data's next flag to next block
					mm.putByteVolatile(dataRelativePos - 1, SharedMMRing.MASK_NEXT_BLOCK);
					// release prev map
					mm.unmap();
					System.out.println("remap to nexBlockStartPos " +nextmm.getStartPos()+" old end pos " + mm.getEndPos());

				}
				mm = curMappingMM;
				while (mm.getStartPos() < writeStartPos) {
					Thread.yield();
					mm = curMappingMM;
				}
				dataRelativePos = mm.getRelovePos(nextWritePos);
				// write data
				this.writeMsg(mm, dataRelativePos, rawMsg);
			}
			// update prev data's next flag
			mm.putByteVolatile(dataRelativePos - 1, SharedMMRing.FLAG_NEXT_ADJACENT);
			return 1;

		} else {
			return -1;
			// throw new RuntimeException("file is full " + this.loc);
		}

	}

	private void writeMsg(UnsafeMemory mm, long pos, byte[] data) {

		short msgLength = (short) data.length;
		mm.putShort(pos, msgLength);
		mm.setBytes(pos + 2, data, 0, msgLength);
		mm.putByte(pos + 2 + msgLength, SharedMMRing.FLAG_NO_NEXT);

	}

	public long getNextDataAddr() {
		return fileHeaderMM.getLongVolatile(0);

	}

	public long getWriteStartAddr() {
		return fileHeaderMM.getLongVolatile(8);

	}

	private int getMsgTotalSpace(byte[] msg) {
		return msg.length + MSG_PADDING_LENGTH;
	}
}
