package com.hamersaw.distributed_file_system.message;

public class ControllerHeartbeatMsg extends Message {
	@Override
	public int getMsgType() {
		return CONTROLLER_HEARTBEAT_MSG;
	}
}