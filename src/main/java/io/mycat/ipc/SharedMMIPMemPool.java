package io.mycat.ipc;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

/**
 * an whole shared memory buffer pool first two byte means allocated count of
 * queues, then each queue's define: 2 bytes: this queue's group 4 bytes: this
 * queque's capacity queue's memory
 * 
 * @author wuzhih
 *
 */
public class SharedMMIPMemPool {
	private final long addr, size;
	private final String loc;
	private UnsafeMemory mm;
	private final int MAX_QUEUE_COUNT = 2048;
	private final int MM_QUEUE_START = 2;
	private final int MM_QUEUE_METADATA_LEN = 2 + 4;
	private Map<Short, SharedMMRing> allocateRings = new HashMap<Short, SharedMMRing>();
	private volatile SharedMMRing lastRing;

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
	protected SharedMMIPMemPool(final String loc, long len, boolean createNewFile) throws Exception {
		if (createNewFile) {
			new File(loc).delete();
		}
		this.loc = loc;
		this.size = roundTo4096(len);
		this.addr = UnsafeMemory.mapAndSetOffset(loc, size);
		this.mm = new UnsafeMemory(addr, size);
		init();
	}

	
	public String getLoc() {
		return loc;
	}

	private void init() {
		short curQueueCount = getQueueCountInMM();
		int addr = MM_QUEUE_START;
		SharedMMRing latest = null;
		long queueAddr = getFirstQueueAddr();
		for (int i = 0; i < curQueueCount; i++) {
			short group = mm.getShortVolatile(addr);
			addr += 4;
			int rawLength = mm.getIntVolatile(addr);
			QueueMeta meta = new QueueMeta(group, rawLength, queueAddr);
			queueAddr += rawLength;
			SharedMMRing ring = new SharedMMRing(meta,mm.getAddr());
			allocateRings.put(Short.valueOf(group), ring);
			latest = ring;
		}
		this.lastRing = latest;
		mm.getLong(12290);
	}

	private long getFirstQueueAddr() {
		return MM_QUEUE_START + MM_QUEUE_METADATA_LEN * MAX_QUEUE_COUNT;
	}

	public SharedMMRing createNewRing(short groupId, int rawLength) {
		short curQueueCount = getQueueCountInMM();
		if (curQueueCount >= MAX_QUEUE_COUNT) {
			return null;
		}
		int metaAddr = MM_QUEUE_START + MM_QUEUE_METADATA_LEN * curQueueCount;
		// write queue metedata info
		mm.putShortVolatile(metaAddr, groupId);
		metaAddr += 2;
		mm.putIntVolatile(metaAddr, rawLength);
		// create queue
		SharedMMRing prevQueue = this.lastRing;
		long queueAddr = (prevQueue == null) ? getFirstQueueAddr() : prevQueue.getMetaData().getAddrEnd();
		QueueMeta meta = new QueueMeta(groupId, rawLength, queueAddr);
		SharedMMRing ring = new SharedMMRing(meta,mm.getAddr());
		// update header
		mm.putShortVolatile(0, ++curQueueCount);
		// put map
		allocateRings.put(Short.valueOf(groupId), ring);
		this.lastRing = ring;
		return ring;
	}

	public SharedMMRing getLastRing() {
		return this.lastRing;
	}

	public short getQueueCountInMM() {
		return mm.getShortVolatile(0);
	}

	private static long roundTo4096(long i) {
		return (i + 0xfffL) & ~0xfffL;
	}

}
