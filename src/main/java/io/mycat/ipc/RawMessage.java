package io.mycat.ipc;

import java.nio.ByteBuffer;

public class RawMessage {

	private short msgLength;
	private byte[] rawData;
	private byte dataFlag;

	public RawMessage() {

	}

	public byte[] getRawData() {
		return rawData;
	}

	public byte getDataFlag() {
		return this.dataFlag;
	}

	public RawMessage(byte[] rawData) {
		super();
		this.rawData = rawData;
		this.msgLength = (short) rawData.length;
	}

	

}
