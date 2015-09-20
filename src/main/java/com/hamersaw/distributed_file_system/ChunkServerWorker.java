package com.hamersaw.distributed_file_system;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import java.net.Socket;

import java.util.LinkedList;
import java.util.logging.Logger;

import com.hamersaw.distributed_file_system.message.ControllerHeartbeatMsg;
import com.hamersaw.distributed_file_system.message.ErrorMsg;
import com.hamersaw.distributed_file_system.message.ForwardChunkMsg;
import com.hamersaw.distributed_file_system.message.Message;
import com.hamersaw.distributed_file_system.message.RequestChunkMsg;
import com.hamersaw.distributed_file_system.message.ReplyChunkMsg;
import com.hamersaw.distributed_file_system.message.SuccessMsg;
import com.hamersaw.distributed_file_system.message.WriteChunkMsg;

public class ChunkServerWorker implements Runnable {
	private static Logger LOGGER = Logger.getLogger(ChunkServerWorker.class.getCanonicalName());
	protected Socket socket;		
	protected  ChunkServer chunkServer;

	public ChunkServerWorker(Socket socket, ChunkServer chunkServer) {
		this.socket = socket;
		this.chunkServer = chunkServer;
	}

	@Override
	public void run() {
		try {
			//read request message
			ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
			Message requestMsg = (Message) in.readObject();

			Message replyMsg = null;
			try {
				switch(requestMsg.getMsgType()) {
				case Message.CONTROLLER_HEARTBEAT_MSG:
					break;
				case Message.WRITE_CHUNK_MSG:
					LOGGER.info("Received message of type 'WRITE_CHUNK_MSG' from '" + socket.getInetAddress() + ":" + socket.getPort());
					WriteChunkMsg wcMsg = (WriteChunkMsg) requestMsg;
					chunkServer.writeChunk(wcMsg.getFilename(), wcMsg.getChunkNum(), wcMsg.getLength(), wcMsg.getBytes(), wcMsg.getEof(), wcMsg.getTimestamp());
	
					//forward message to other chunk servers
					if(!wcMsg.getChunkServers().isEmpty()) {
						ChunkServerMetadata chunkServer = wcMsg.getChunkServers().get(0);
						wcMsg.getChunkServers().remove(0);
					
						Socket clientSocket = new Socket(chunkServer.getInetAddress(), chunkServer.getPort());
						ObjectOutputStream socketOut = new ObjectOutputStream(clientSocket.getOutputStream());
						socketOut.writeObject(wcMsg);

						clientSocket.close();
					}

					break;
				case Message.REQUEST_CHUNK_MSG:
					LOGGER.info("Received message of type 'REQUEST_CHUNK_MSG' from '" + socket.getInetAddress() + ":" + socket.getPort());
					RequestChunkMsg rcMsg = (RequestChunkMsg) requestMsg;

					try {
						replyMsg = chunkServer.requestChunk(rcMsg.getFilename(), rcMsg.getChunkNum());
					} catch(Exception e) {
						replyMsg = new ErrorMsg(e.getMessage());
					}

					break;
				case Message.FORWARD_CHUNK_MSG:
					LOGGER.info("Received message of type 'FORWARD_CHUNK_MSG' from '" + socket.getInetAddress() + ":" + socket.getPort());
					ForwardChunkMsg fcMsg = (ForwardChunkMsg) requestMsg;

					try {
						//form a write chunk message to send to the other server
						ReplyChunkMsg recMsg = (ReplyChunkMsg) chunkServer.requestChunk(fcMsg.getFilename(), fcMsg.getChunkNum());
						WriteChunkMsg writeChunkMsg = new WriteChunkMsg(
							recMsg.getFilename(),
							recMsg.getChunkNum(),
							recMsg.getLength(),
							recMsg.getBytes(),
							recMsg.getEof(),
							recMsg.getTimestamp(),
							new LinkedList<ChunkServerMetadata>()
						);

						Socket clientSocket = new Socket(fcMsg.getInetAddress(), fcMsg.getPort());
						ObjectOutputStream socketOut = new ObjectOutputStream(clientSocket.getOutputStream());
						socketOut.writeObject(writeChunkMsg);

						clientSocket.close();

						replyMsg = new SuccessMsg();
					} catch(Exception e) {
						replyMsg = new ErrorMsg(e.getMessage());
					}

					break;
				default:
					LOGGER.severe("Unrecognized request message type '" + requestMsg.getMsgType() + "'");
					break;
				}
			} catch(Exception e) {
				LOGGER.severe(e.getMessage());
			}

			//write reply message
			if(replyMsg != null) {
				ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
				out.writeObject(replyMsg);
			}

			//close client socket
			socket.close();
		} catch(Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
}
