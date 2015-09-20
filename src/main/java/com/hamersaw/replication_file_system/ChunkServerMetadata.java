package com.hamersaw.replication_file_system;

import java.io.Serializable;

import java.net.InetAddress;

public class ChunkServerMetadata implements Comparable<ChunkServerMetadata>, Serializable {
	private InetAddress inetAddress;
	private int port, totalChunks;
	private long freeSpace;

	public ChunkServerMetadata(InetAddress inetAddress, int port, int totalChunks, long freeSpace) {
		this.inetAddress = inetAddress;
		this.port = port;
		this.totalChunks = totalChunks;
		this.freeSpace = freeSpace;
	}

	@Override
	public int compareTo(ChunkServerMetadata chunkServerMetadata) {
		//compare inetAddress
		int hostAddress = inetAddress.getHostAddress().compareTo(chunkServerMetadata.getInetAddress().getHostAddress());
		int hostName = inetAddress.getHostName().compareTo(chunkServerMetadata.getInetAddress().getHostName());
		if(hostAddress != 0 && hostName != 0)
			return hostAddress;

		//compare port
		if(port < chunkServerMetadata.getPort()) {
			return -1;
		} else if(port > chunkServerMetadata.getPort()) {
			return 1;
		} else {
			return 0;
		}
	}

	@Override
	public String toString() {
		return inetAddress + ":" + port;
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

	public void setTotalChunks(int totalChunks) {
		this.totalChunks = totalChunks;
	}

	public int getTotalChunks() {
		return totalChunks;
	}

	public void setFreeSpace(long freeSpace) {
		this.freeSpace = freeSpace;
	}

	public long getFreeSpace() {
		return freeSpace;
	}
}
