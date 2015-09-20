package com.hamersaw.replication_file_system;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import java.net.Socket;

import java.util.List;
import java.util.logging.Logger;

import com.hamersaw.replication_file_system.message.ErrorMsg;
import com.hamersaw.replication_file_system.message.ChunkServerHeartbeatMsg;
import com.hamersaw.replication_file_system.message.DataCorruptionMsg;
import com.hamersaw.replication_file_system.message.ForwardChunkMsg;
import com.hamersaw.replication_file_system.message.Message;
import com.hamersaw.replication_file_system.message.RequestChunkServerMsg;
import com.hamersaw.replication_file_system.message.ReplyChunkServerMsg;

public class ControllerWorker implements Runnable {
	private static Logger LOGGER = Logger.getLogger(ControllerWorker.class.getCanonicalName());
	protected Socket socket;		
	protected Controller controller;

	public ControllerWorker(Socket socket, Controller controller) {
		this.socket = socket;
		this.controller = controller;
	}

	@Override
	public void run() {
		try {
			//read request message
			ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
			Message requestMsg = (Message) in.readObject();

			LOGGER.fine("Received message of type '" + requestMsg.getMsgType() + "' from '" + socket.getInetAddress() + ":" + socket.getPort());

			Message replyMsg = null;
			try {
				switch(requestMsg.getMsgType()) {
				case Message.REQUEST_CHUNK_SERVER_MSG:
					LOGGER.info("Received message of type 'REQUEST_CHUNK_SERVER_MSG' from '" + socket.getInetAddress() + ":" + socket.getPort());
					RequestChunkServerMsg rcsMsg = (RequestChunkServerMsg) requestMsg;

					try {
						replyMsg = new ReplyChunkServerMsg(controller.requestChunkServers(rcsMsg.getFilename(), rcsMsg.getChunkNum(), rcsMsg.getWriteOperation()));
					} catch(Exception e) {
						replyMsg = new ErrorMsg(e.getMessage());
					}

					break;
				case Message.CHUNK_SERVER_HEARTBEAT_MSG:
					ChunkServerHeartbeatMsg cshMsg = (ChunkServerHeartbeatMsg) requestMsg;
					cshMsg.getChunkServerMetadata().setInetAddress(socket.getInetAddress()); //TODO fix this up
					controller.updateChunkServer(cshMsg.getChunkServerMetadata(), cshMsg.getChunks());
					break;
				case Message.DATA_CORRUPTION_MSG:
					LOGGER.info("Received message of type 'DATA_CORRUPTION_MSG' from '" + socket.getInetAddress() + ":" + socket.getPort());
					DataCorruptionMsg dcMsg = (DataCorruptionMsg) requestMsg;

					//remove data chunk from controller metadata
					controller.removeChunk(dcMsg.getFilename(), dcMsg.getChunkNum(), socket.getInetAddress(), dcMsg.getPort());

					//find a valid server to send the chunk
					ForwardChunkMsg fcMsg = new ForwardChunkMsg(dcMsg.getFilename(), dcMsg.getChunkNum(), socket.getInetAddress(), dcMsg.getPort());
					List<ChunkServerMetadata> list = controller.requestChunkServers(fcMsg.getFilename(), fcMsg.getChunkNum(), false);
					boolean success = false;
					for(ChunkServerMetadata chunkServerMetadata : list) {
						int hostName = socket.getInetAddress().getHostName().compareTo(chunkServerMetadata.getInetAddress().getHostName());
						int hostAddress = socket.getInetAddress().getHostAddress().compareTo(chunkServerMetadata.getInetAddress().getHostAddress());

						if(fcMsg.getPort() == chunkServerMetadata.getPort() && (hostName == 0 || hostAddress == 0)) {
							continue;
						}

						//send forward chunk message
						Socket clientSocket = new Socket(chunkServerMetadata.getInetAddress(), chunkServerMetadata.getPort());
						ObjectOutputStream socketOut = new ObjectOutputStream(clientSocket.getOutputStream());
						socketOut.writeObject(fcMsg);

						//read response messasge
						ObjectInputStream socketIn = new ObjectInputStream(clientSocket.getInputStream());
						Message message = (Message) socketIn.readObject();

						if(message.getMsgType() == Message.ERROR_MSG) {
							LOGGER.severe(((ErrorMsg)message).getMsg());
							continue;
						} else if(message.getMsgType() != Message.SUCCESS_MSG) {
							LOGGER.severe("Unexpected message type returned. Was expecting '" + Message.SUCCESS_MSG + "' and recieved '" + message.getMsgType() + "'");
							continue;
						}

						clientSocket.close();
						success = true;
						break;
					}

					if(!success) {
						LOGGER.severe(
							"Unable to forward chunk '" 
							+ fcMsg.getFilename() + ":" + fcMsg.getChunkNum() 
							+ "' to chunk server '" 
							+ fcMsg.getInetAddress() + ":" + fcMsg.getPort() + "'"
						);
					}

					break;
				default:
					LOGGER.severe("Unrecognized request message type '" + requestMsg.getMsgType() + "'");
					break;
				}
			} catch(Exception e) {
				e.printStackTrace();
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
