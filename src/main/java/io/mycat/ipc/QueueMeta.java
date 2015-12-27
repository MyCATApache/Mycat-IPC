package io.mycat.ipc;

public class QueueMeta {
private final short groupId;
private final int rawLenth;
private final long addr;
public QueueMeta(short groupId, int rawLenth, long addr) {
	super();
	this.groupId = groupId;
	this.rawLenth = rawLenth;
	this.addr = addr;
}
public long getAddrEnd()
{
	return addr+rawLenth;
}
public short getGroupId() {
	return groupId;
}
public int getRawLenth() {
	return rawLenth;
}
public long getAddr() {
	return addr;
}
@Override
public int hashCode() {
	final int prime = 31;
	int result = 1;
	result = prime * result + (int) (addr ^ (addr >>> 32));
	result = prime * result + groupId;
	result = prime * result + rawLenth;
	return result;
}
@Override
public boolean equals(Object obj) {
	if (this == obj)
		return true;
	if (obj == null)
		return false;
	if (getClass() != obj.getClass())
		return false;
	QueueMeta other = (QueueMeta) obj;
	if (addr != other.addr)
		return false;
	if (groupId != other.groupId)
		return false;
	if (rawLenth != other.rawLenth)
		return false;
	return true;
}

}
