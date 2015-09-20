package com.hamersaw.replication_file_system.message;

import java.net.InetAddress;

public class DataCorruptionMsg extends Message {
	private String filename;
	private int chunkNum, port;
	private InetAddress inetAddress;

	public DataCorruptionMsg(String filename, int chunkNum, InetAddress inetAddress, int port) {
		this.filename = filename;
		this.chunkNum = chunkNum;
		this.inetAddress = inetAddress;
		this.port = port;
	}

	public String getFilename() {
		return filename;
	}

	public int getChunkNum() {
		return chunkNum;
	}

	public void setInetAddress(InetAddress inetAddress) {
		this.inetAddress = inetAddress;
	}

	public InetAddress getInetAddress() {
		return inetAddress;
	}

	public int getPort() {
		return port;
	}

	@Override
	public int getMsgType() {
		return DATA_CORRUPTION_MSG;
	}
}
