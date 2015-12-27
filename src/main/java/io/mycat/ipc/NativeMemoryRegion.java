package io.mycat.ipc;

public class NativeMemoryRegion {

	private final UnsafeMemory mm;
	private final long addr, size;
	public NativeMemoryRegion(UnsafeMemory mm, long addr, long size) {
		super();
		this.mm = mm;
		this.addr = addr;
		this.size = size;
	}
	public UnsafeMemory getMm() {
		return mm;
	}
	public long getAddr() {
		return addr;
	}
	public long getSize() {
		return size;
	}
	
}
