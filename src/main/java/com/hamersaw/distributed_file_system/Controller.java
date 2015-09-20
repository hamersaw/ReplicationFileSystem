package com.hamersaw.distributed_file_system;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;
import java.util.Map;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;

import com.hamersaw.distributed_file_system.message.ControllerHeartbeatMsg;
import com.hamersaw.distributed_file_system.message.ErrorMsg;
import com.hamersaw.distributed_file_system.message.ForwardChunkMsg;
import com.hamersaw.distributed_file_system.message.Message;
import com.hamersaw.distributed_file_system.message.RequestChunkServerMsg;
import com.hamersaw.distributed_file_system.message.ReplyChunkServerMsg;

public class Controller implements Runnable {
	private static final Logger LOGGER = Logger.getLogger(Controller.class.getCanonicalName());
	protected int port, replicationCount;
	protected boolean stopped;
	protected ServerSocket serverSocket;
	protected List<ChunkServerMetadata> chunkServers;
	protected Map<String,Map<Integer,List<ChunkServerMetadata>>> chunks;

	public Controller(int port, int replicationCount) {
		this.port = port;
		this.replicationCount = replicationCount;
		stopped = false;
		chunkServers = new LinkedList<ChunkServerMetadata>();
		chunks = new HashMap<String,Map<Integer,List<ChunkServerMetadata>>>();
	}

	public static void main(String[] args) {
		try {
			int port = Integer.parseInt(args[0]);
			int replicationCount = Integer.parseInt(args[1]);

			new Thread(new Controller(port, replicationCount)).start();
		} catch(Exception e) {
			System.out.println("Usage: Controller port replicationCount");
			System.exit(1);
		}
	}

	@Override
	public void run() {
		try{
			ServerSocket serverSocket = new ServerSocket(port);
			LOGGER.info("Controller started successfully");
			
			//start HeartbeatTask that executes every 30 seconds
			Timer timer = new Timer();
			timer.schedule(new HeartbeatTask(), 0, 30 * 1000);

			while(!stopped) {
				Socket socket = serverSocket.accept();
				LOGGER.fine("Received connection from '" + socket.getInetAddress() + ":" + socket.getPort() + "'.");

				new Thread(
					new ControllerWorker(
						socket,
						this
					)
				).start();
			}
		} catch(Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	public synchronized void updateChunkServer(ChunkServerMetadata chunkServerMetadata, Map<String,List<Integer>> chunkServerChunks) {
		//search for chunk server
		boolean found = false;
		for(ChunkServerMetadata chunkServer : chunkServers) {
			if(chunkServer.compareTo(chunkServerMetadata) == 0) {
				chunkServer.setTotalChunks(chunkServerMetadata.getTotalChunks());
				chunkServer.setFreeSpace(chunkServerMetadata.getFreeSpace());
				chunkServerMetadata = chunkServer;
				found = true;
				break;
			}
		}

		if(!found) {
			LOGGER.info("Adding ChunkServer '" + chunkServerMetadata + "'");
			chunkServers.add(chunkServerMetadata);
		}

		//update chunks
		for(String filename : chunkServerChunks.keySet()) {
			for(int chunkNum : chunkServerChunks.get(filename)) {
				//search for filename
				Map<Integer,List<ChunkServerMetadata>> chunkNums = chunks.get(filename);
				if(chunks.containsKey(filename)) {
					chunkNums = chunks.get(filename);
				} else {
					chunkNums = new HashMap<Integer,List<ChunkServerMetadata>>();
					chunks.put(filename, chunkNums);
				}

				//search for chunk number
				List<ChunkServerMetadata> chunkServers;
				if(chunkNums.containsKey(chunkNum)) {
					chunkServers = chunkNums.get(chunkNum);
				} else {
					chunkServers = new LinkedList<ChunkServerMetadata>();
					chunkNums.put(chunkNum, chunkServers);
				}

				//add chunk server if needed
				if(!chunkServers.contains(chunkServerMetadata)) {
					LOGGER.info("Chunk '" + filename + ":" + chunkNum + "' registered for chunk server '" + chunkServerMetadata + "'");
					chunkServers.add(chunkServerMetadata);
				}
			}
		}
	}

	public void removeChunk(String filename, int chunkNum, InetAddress inetAddress, int port) throws Exception {
		Map<Integer,List<ChunkServerMetadata>> map;
		if(chunks.containsKey(filename)) {
			map = chunks.get(filename);
		} else {
			throw new Exception("File '" + filename + "' not found in chunk cache");
		}

		List<ChunkServerMetadata> list;
		if(map.containsKey(chunkNum)) {
			list = map.get(chunkNum);
		} else {
			throw new Exception("Chunk '" + filename + ":" + chunkNum + "' not found in chunk cache");
		}

		//loop through chunk servers to delete
		for(int i=0; i<list.size(); i++) {
			ChunkServerMetadata chunkServerMetadata = list.get(i);
			int hostName = inetAddress.getHostName().compareTo(chunkServerMetadata.getInetAddress().getHostName());
			int hostAddress = inetAddress.getHostAddress().compareTo(chunkServerMetadata.getInetAddress().getHostAddress());

			if(port == chunkServerMetadata.getPort() && (hostName == 0 || hostAddress == 0)) {
				list.remove(i);
				break;
			}
		}
	}

	public List<ChunkServerMetadata> requestChunkServers(String filename, int chunkNum, boolean writeOperation) throws Exception{
		List<ChunkServerMetadata> list = new LinkedList<ChunkServerMetadata>();
		if(chunks.containsKey(filename) && chunks.get(filename).containsKey(chunkNum)) {
			for(ChunkServerMetadata chunkServerMetadata : chunks.get(filename).get(chunkNum)) {
				list.add(chunkServerMetadata);
			}
		} else if(!writeOperation) {
			throw new Exception("Chunk '" + filename + ":" + chunkNum + "' not found in any chunk servers.");
		} else if(chunkServers.size() < replicationCount) {
			throw new Exception("Not enough chunk servers to fulfil replication requirement. '" + chunkServers.size() + "' found '" + replicationCount + "' required.");
		} else {
			Collections.sort(
				chunkServers,
				new Comparator<ChunkServerMetadata>() {
					@Override
					public int compare(ChunkServerMetadata c1, ChunkServerMetadata c2) {
						if(c1.getFreeSpace() > c2.getFreeSpace()) {
							return -1;
						} else if(c1.getFreeSpace() < c2.getFreeSpace()) {
							return 1;
						} else {
							return 0;
						}
					}
				}
			);

			List<ChunkServerMetadata> chunkServerList = new LinkedList<ChunkServerMetadata>();
			for(int i=0; i<replicationCount; i++) {
				list.add(chunkServers.get(i));
			}
		}

		//update free space on chunk server if write operation
		if(writeOperation) {
			for(ChunkServerMetadata chunkServerMetadata : list) {
				chunkServerMetadata.setFreeSpace(chunkServerMetadata.getFreeSpace() - 1);
			}
		}

		return list;
	}

	public synchronized void stop() {
		stopped = true;
	}

	public boolean getStopped() {
		return stopped;
	}

	class HeartbeatTask extends TimerTask {
		private Random rand;

		public HeartbeatTask() {
			rand = new Random();
		}

		@Override
		public void run() {
			List<ChunkServerMetadata> unreachableList = new LinkedList<ChunkServerMetadata>();
			for(ChunkServerMetadata chunkServerMetadata : chunkServers) {
				try {
					//open a socket
					Socket socket = new Socket(chunkServerMetadata.getInetAddress(), chunkServerMetadata.getPort());

					//send a heartbeat message
					ControllerHeartbeatMsg message = new ControllerHeartbeatMsg();
					ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
					out.writeObject(message);
				} catch(Exception e) {
					LOGGER.info("Remove chunk server '" + chunkServerMetadata + "'. Host unreachable.");

					unreachableList.add(chunkServerMetadata);
				}
			}

			//remove unreachable chunk servers
			for(ChunkServerMetadata chunkServerMetadata : unreachableList) {
				chunkServers.remove(chunkServerMetadata);

				for(String filename : chunks.keySet()) {
					for(int chunkNum : chunks.get(filename).keySet()) {
						List<ChunkServerMetadata> list = chunks.get(filename).get(chunkNum);

						if(list.contains(chunkServerMetadata)) {
							list.remove(chunkServerMetadata);

							try {
								ChunkServerMetadata forwardChunkServer = list.get(rand.nextInt(list.size()));
							
								boolean success = false;
								for(ChunkServerMetadata writeChunkServer : chunkServers) {
									if(list.contains(writeChunkServer)) {
										continue;
									}

									ForwardChunkMsg fcMsg = new ForwardChunkMsg(filename, chunkNum, writeChunkServer.getInetAddress(), writeChunkServer.getPort());

									//send forward chunk message
									Socket clientSocket = new Socket(forwardChunkServer.getInetAddress(), forwardChunkServer.getPort());
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
									LOGGER.severe("Unable to redistribute chunk '" + filename + ":" + chunkNum + "'");
								}
							} catch(Exception e) {
								LOGGER.severe(e.getMessage());
							}
						}
					}
				}
			}
		}
	}
}
