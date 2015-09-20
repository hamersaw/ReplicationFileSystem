package com.hamersaw.distributed_file_system.message;

import java.util.List;

import com.hamersaw.distributed_file_system.ChunkServerMetadata;

public class WriteChunkMsg extends Message {
	private String filename;
	private int chunkNum, length;
	private byte[] bytes;
	private boolean eof;
	private long timestamp;
	private List<ChunkServerMetadata> chunkServers;

	public WriteChunkMsg(String filename, int chunkNum, int length, byte[] bytes, boolean eof, long timestamp, List<ChunkServerMetadata> chunkServers) {
		this.filename = filename;
		this.chunkNum = chunkNum;
		this.length = length;
		this.bytes = bytes;
		this.eof = eof;
		this.timestamp = timestamp;
		this.chunkServers = chunkServers;
	}

	public String getFilename() {
		return filename;
	}

	public int getChunkNum() {
		return chunkNum;
	}

	public int getLength() {
		return length;
	}

	public byte[] getBytes() {
		return bytes;
	}

	public boolean getEof() {
		return eof;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public List<ChunkServerMetadata> getChunkServers() {
		return chunkServers;
	}

	@Override
	public int getMsgType() {
		return WRITE_CHUNK_MSG;
	}
}
