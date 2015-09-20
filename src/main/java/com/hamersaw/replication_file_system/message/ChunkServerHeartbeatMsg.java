package com.hamersaw.replication_file_system.message;

import java.net.InetAddress;

import java.util.List;
import java.util.Map;

import com.hamersaw.replication_file_system.ChunkServerMetadata;

public class ChunkServerHeartbeatMsg extends Message {
	private ChunkServerMetadata chunkServerMetadata;
	private Map<String,List<Integer>> chunks;	

	public ChunkServerHeartbeatMsg(ChunkServerMetadata chunkServerMetadata) {
		this.chunkServerMetadata = chunkServerMetadata;
	}

	public ChunkServerMetadata getChunkServerMetadata() {
		return chunkServerMetadata;
	}

	public void setChunks(Map<String,List<Integer>> chunks) {
		this.chunks = chunks;
	}

	public Map<String,List<Integer>> getChunks() {
		return chunks;
	}

	@Override
	public int getMsgType() {
		return CHUNK_SERVER_HEARTBEAT_MSG;
	}
}
