package com.hamersaw.distributed_file_system.message;

public class SuccessMsg extends Message {
	public int getMsgType() {
		return SUCCESS_MSG;
	}
}
