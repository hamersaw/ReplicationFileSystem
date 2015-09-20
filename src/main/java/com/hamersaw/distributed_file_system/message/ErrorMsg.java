package com.hamersaw.distributed_file_system.message;

public class ErrorMsg extends Message {
	protected String msg;

	public ErrorMsg(String msg) {
		this.msg = msg;
	}

	public String getMsg() {
		return msg;
	}

	@Override
	public int getMsgType() {
		return ERROR_MSG;
	}
}
