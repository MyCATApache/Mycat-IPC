package io.mycat.ipc;

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
	private final String loc;
	private UnsafeMemory mm;
	private final int MAX_QUEUE_COUNT = 2048;
	private final int MM_QUEUE_START = 2;
	private final int MM_QUEUE_METADATA_LEN = 2 + 4 + 1;
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
	public SharedMMIPMemPool(final String loc, long len, boolean createNewFile) throws Exception {

		this.loc = loc;
		this.mm = UnsafeMemory.mapAndSetOffset(loc, len, createNewFile, 0, len);
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
			addr += 2;
			int rawLength = mm.getIntVolatile(addr);
			addr += 4;
			byte storageType = mm.getByteVolatile(addr);
			addr += 1;
			QueueMeta meta = new QueueMeta(group, rawLength, queueAddr, storageType);
			queueAddr += rawLength;
			SharedMMRing ring = new SharedMMRing(meta, mm.getAddr());
			allocateRings.put(group, ring);
			latest = ring;
		}
		this.lastRing = latest;

	}

	private long getFirstQueueAddr() {
		return MM_QUEUE_START + MM_QUEUE_METADATA_LEN * MAX_QUEUE_COUNT;
	}

	public synchronized SharedMMRing createNewRing(short groupId, int rawLength, byte storageType) {
		short curQueueCount = getQueueCountInMM();
		if (curQueueCount >= MAX_QUEUE_COUNT) {
			return null;
		}
		int metaAddr = MM_QUEUE_START + MM_QUEUE_METADATA_LEN * curQueueCount;
		// write queue metedata info
		mm.putShortVolatile(metaAddr, groupId);
		metaAddr += 2;
		mm.putIntVolatile(metaAddr, rawLength);
		metaAddr += 4;
		mm.putByteVolatile(metaAddr, storageType);
		// create queue
		SharedMMRing prevQueue = this.lastRing;
		long queueAddr = (prevQueue == null) ? getFirstQueueAddr() : prevQueue.getMetaData().getAddrEnd();
		QueueMeta meta = new QueueMeta(groupId, rawLength, queueAddr, storageType);
		SharedMMRing ring = new SharedMMRing(meta,mm.getAddr());
		// update header
		mm.putShortVolatile(0, ++curQueueCount);
		// put map
		allocateRings.put(groupId, ring);
		this.lastRing = ring;
		return ring;
	}

	public SharedMMRing getRing(short i) {
		return allocateRings.get(i);
	}

	public SharedMMRing getLastRing() {
		return this.lastRing;
	}

	public short getQueueCountInMM() {
		return mm.getShortVolatile(0);
	}

}
