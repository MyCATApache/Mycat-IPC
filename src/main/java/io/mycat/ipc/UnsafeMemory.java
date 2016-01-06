package io.mycat.ipc;

import java.io.File;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.channels.FileChannel;

import io.mycat.ipc.util.Util;
import sun.misc.Unsafe;
import sun.nio.ch.FileChannelImpl;

@SuppressWarnings("restriction")
public class UnsafeMemory {
	public static final Unsafe unsafe;
	public static final Method mmap;
	public static final Method unmmap;
	public static final int BYTE_ARRAY_OFFSET;

	private final long addr, startPos, size;

	static {
		try {
			Field singleoneInstanceField = Unsafe.class.getDeclaredField("theUnsafe");
			singleoneInstanceField.setAccessible(true);
			unsafe = (Unsafe) singleoneInstanceField.get(null);
			mmap = getMethod(FileChannelImpl.class, "map0", int.class, long.class, long.class);
			mmap.setAccessible(true);
			unmmap = getMethod(FileChannelImpl.class, "unmap0", long.class, long.class);
			unmmap.setAccessible(true);
			BYTE_ARRAY_OFFSET = unsafe.arrayBaseOffset(byte[].class);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public static UnsafeMemory mapAndSetOffset(final String loc, long fileSize, boolean createNewFile, long startPos,
			long mapSize) throws Exception {
		long size = 0;
		RandomAccessFile backingFile = new RandomAccessFile(loc, "rw");
		if (createNewFile) {
			new File(loc).delete();
			size = Util.roundTo4096(fileSize);
			backingFile.setLength(size);
		}
		FileChannel ch = backingFile.getChannel();
		long addr = (long) mmap.invoke(ch, 1, startPos, mapSize);
		ch.close();
		backingFile.close();
		return new UnsafeMemory(addr, startPos, mapSize);

	}

	public UnsafeMemory(long addr, long startPos, long size) {
		this.addr = addr;
		this.startPos = startPos;
		this.size = size;
	}

	public long getAddr() {
		return addr;
	}

	public long getEndAddr() {
		return addr + size;
	}
	public long getRelovePos(long abPos)
	{
		return abPos-startPos;
	}

	public long getStartPos()
	{
		return this.startPos;
	}
	public long getEndPos() {
		return size + startPos;
	}

	public long getSize() {
		return size;
	}

	private static Method getMethod(Class<?> cls, String name, Class<?>... params) throws Exception {
		Method m = cls.getDeclaredMethod(name, params);
		m.setAccessible(true);
		return m;
	}

	protected void unmap() throws Exception {
		unmmap.invoke(null, addr, this.size);
	
	}

	/**
	 * Reads a byte from the specified position.
	 * 
	 * @param pos
	 *            the position in the memory mapped file
	 * @return the value read
	 */
	public byte getByte(long pos) {
		return unsafe.getByte(pos + addr);
	}

	/**
	 * Reads a byte (volatile) from the specified position.
	 * 
	 * @param pos
	 *            the position in the memory mapped file
	 * @return the value read
	 */
	protected byte getByteVolatile(long pos) {
		return unsafe.getByteVolatile(null, pos + addr);
	}

	/**
	 * Reads a short from the specified position.
	 * 
	 * @param pos
	 *            the position in the memory mapped file
	 * @return the value read
	 */
	public short getShort(long pos) {
		return unsafe.getShort(pos + addr);
	}

	/**
	 * Reads a short (volatile) from the specified position.
	 * 
	 * @param pos
	 *            the position in the memory mapped file
	 * @return the value read
	 */
	protected short getShortVolatile(long pos) {
		return unsafe.getShortVolatile(null, pos + addr);
	}

	/**
	 * Reads an int from the specified position.
	 * 
	 * @param pos
	 *            the position in the memory mapped file
	 * @return the value read
	 */
	public int getInt(long pos) {
		return unsafe.getInt(pos + addr);
	}

	/**
	 * Reads an int (volatile) from the specified position.
	 * 
	 * @param pos
	 *            position in the memory mapped file
	 * @return the value read
	 */
	protected int getIntVolatile(long pos) {
		return unsafe.getIntVolatile(null, pos + addr);
	}

	/**
	 * Reads a long from the specified position.
	 * 
	 * @param pos
	 *            position in the memory mapped file
	 * @return the value read
	 */
	public long getLong(long pos) {
		return unsafe.getLong(pos + addr);
	}

	/**
	 * Reads a long (volatile) from the specified position.
	 * 
	 * @param pos
	 *            position in the memory mapped file
	 * @return the value read
	 */
	protected long getLongVolatile(long pos) {
		return unsafe.getLongVolatile(null, pos + addr);
	}

	/**
	 * Writes a byte to the specified position.
	 * 
	 * @param pos
	 *            the position in the memory mapped file
	 * @param val
	 *            the value to write
	 */
	public void putByte(long pos, byte val) {
		unsafe.putByte(pos + addr, val);
	}

	/**
	 * Writes a byte (volatile) to the specified position.
	 * 
	 * @param pos
	 *            the position in the memory mapped file
	 * @param val
	 *            the value to write
	 */
	protected void putByteVolatile(long pos, byte val) {
		unsafe.putByteVolatile(null, pos + addr, val);
	}

	/**
	 * Writes an int to the specified position.
	 * 
	 * @param pos
	 *            the position in the memory mapped file
	 * @param val
	 *            the value to write
	 */
	public void putInt(long pos, int val) {
		unsafe.putInt(pos + addr, val);
	}

	/**
	 * Writes an int (volatile) to the specified position.
	 * 
	 * @param pos
	 *            the position in the memory mapped file
	 * @param val
	 *            the value to write
	 */
	protected void putIntVolatile(long pos, int val) {
		unsafe.putIntVolatile(null, pos + addr, val);
	}

	/**
	 * Writes an short to the specified position.
	 * 
	 * @param pos
	 *            the position in the memory mapped file
	 * @param val
	 *            the value to write
	 */
	public void putShort(long pos, short val) {
		unsafe.putShort(null, pos + addr, val);
	}

	/**
	 * Writes an short (volatile) to the specified position.
	 * 
	 * @param pos
	 *            the position in the memory mapped file
	 * @param val
	 *            the value to write
	 */
	protected void putShortVolatile(long pos, short val) {
		unsafe.putShortVolatile(null, pos + addr, val);
	}

	/**
	 * Writes a long to the specified position.
	 * 
	 * @param pos
	 *            the position in the memory mapped file
	 * @param val
	 *            the value to write
	 */
	public void putLong(long pos, long val) {
		unsafe.putLong(pos + addr, val);
	}

	/**
	 * Writes a long (volatile) to the specified position.
	 * 
	 * @param pos
	 *            the position in the memory mapped file
	 * @param val
	 *            the value to write
	 */
	protected void putLongVolatile(long pos, long val) {
		unsafe.putLongVolatile(null, pos + addr, val);

	}

	protected void lazySetLong(long pos, long val) {
		unsafe.putOrderedLong(null, pos + addr, val);

	}

	protected void lazySetInt(long pos, int val) {
		unsafe.putOrderedInt(null, pos + addr, val);

	}

	/**
	 * Reads a buffer of data.
	 * 
	 * @param pos
	 *            the position in the memory mapped file
	 * @param data
	 *            the input buffer
	 * @param offset
	 *            the offset in the buffer of the first byte to read data into
	 * @param length
	 *            the length of the data
	 */
	public void getBytes(long pos, byte[] data, int offset, int length) {
		unsafe.copyMemory(null, pos + addr, data, BYTE_ARRAY_OFFSET + offset, length);
	}

	/**
	 * Writes a buffer of data.
	 * 
	 * @param pos
	 *            the position in the memory mapped file
	 * @param data
	 *            the output buffer
	 * @param offset
	 *            the offset in the buffer of the first byte to write
	 * @param length
	 *            the length of the data
	 */
	public void setBytes(long pos, byte[] data, int offset, int length) {
		unsafe.copyMemory(data, BYTE_ARRAY_OFFSET + offset, null, pos + addr, length);
	}

	protected boolean compareAndSwapInt(long pos, int expected, int value) {
		return unsafe.compareAndSwapInt(null, pos + addr, expected, value);
	}

	protected boolean compareAndSwapLong(long pos, long expected, long value) {
		return unsafe.compareAndSwapLong(null, pos + addr, expected, value);
	}

	protected long getAndAddLong(long pos, long delta) {
		return unsafe.getAndAddLong(null, pos + addr, delta);
	}
}
