package com.hamersaw.replication_file_system.message;

public class ReplyChunkMsg extends Message {
	private String filename;
	private int chunkNum, length;
	private byte[] bytes;
	private boolean eof;
	private long timestamp;

	public ReplyChunkMsg(String filename, int chunkNum, int length, byte[] bytes, boolean eof, long timestamp) {
		this.filename = filename;
		this.chunkNum = chunkNum;
		this.length = length;
		this.bytes = bytes;
		this.eof = eof;
		this.timestamp = timestamp;
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

	@Override
	public int getMsgType() {
		return REPLY_CHUNK_MSG;
	}
}
