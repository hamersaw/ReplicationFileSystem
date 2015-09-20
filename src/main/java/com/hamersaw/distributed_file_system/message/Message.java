package com.hamersaw.distributed_file_system.message;

import java.io.Serializable;

public abstract class Message implements Serializable {
	public static final int ERROR_MSG = 0,
		REQUEST_CHUNK_SERVER_MSG = 1,
		REPLY_CHUNK_SERVER_MSG = 2,
		CHUNK_SERVER_HEARTBEAT_MSG = 3,
		CONTROLLER_HEARTBEAT_MSG = 4,
		WRITE_CHUNK_MSG = 5,
		REQUEST_CHUNK_MSG = 6,
		REPLY_CHUNK_MSG = 7,
		DATA_CORRUPTION_MSG = 8,
		FORWARD_CHUNK_MSG = 9,
		SUCCESS_MSG = 10;

	public abstract int getMsgType();
}
