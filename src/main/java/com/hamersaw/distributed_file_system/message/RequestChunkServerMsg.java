package com.hamersaw.distributed_file_system.message;

public class RequestChunkServerMsg extends Message {
	private String filename;
	private int chunkNum;
	private boolean writeOperation;

	public RequestChunkServerMsg(String filename, int chunkNum, boolean writeOperation) {
		this.filename = filename;
		this.chunkNum = chunkNum;
		this.writeOperation = writeOperation;
	}

	public String getFilename() {
		return filename;
	}

	public int getChunkNum() {
		return chunkNum;
	}

	public boolean getWriteOperation() {
		return writeOperation;
	}

	@Override
	public int getMsgType() {
		return REQUEST_CHUNK_SERVER_MSG;
	}
}
