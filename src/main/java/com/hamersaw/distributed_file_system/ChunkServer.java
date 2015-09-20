package com.hamersaw.distributed_file_system;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

import java.security.MessageDigest;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import com.hamersaw.distributed_file_system.message.ErrorMsg;
import com.hamersaw.distributed_file_system.message.Message;
import com.hamersaw.distributed_file_system.message.ChunkServerHeartbeatMsg;
import com.hamersaw.distributed_file_system.message.DataCorruptionMsg;
import com.hamersaw.distributed_file_system.message.ForwardChunkMsg;
import com.hamersaw.distributed_file_system.message.ReplyChunkMsg;
import com.hamersaw.distributed_file_system.message.ReplyChunkServerMsg;
import com.hamersaw.distributed_file_system.message.RequestChunkMsg;
import com.hamersaw.distributed_file_system.message.RequestChunkServerMsg;

class ChunkServer implements Runnable {
	private static final Logger LOGGER = Logger.getLogger(ChunkServer.class.getCanonicalName());
	public static final int DIGEST_LENGTH = 8096;
	protected String storageDirectory, controllerHostName;
	protected int controllerPort, port;
	protected boolean stopped;
	protected Map<String,List<Integer>> chunks, newChunks;
	protected ServerSocket serverSocket;

	public ChunkServer(String storageDirectory, String controllerHostName, int controllerPort, int port) {
		this.storageDirectory = storageDirectory;
		this.controllerHostName = controllerHostName;
		this.controllerPort = controllerPort;
		this.port = port;
		stopped = false;
		chunks = new HashMap<String,List<Integer>>();
		newChunks = new HashMap<String,List<Integer>>();
	}

	public static void main(String[] args) {
		try {
			String storageDirectory = args[0];
			String controllerHostName = args[1];
			int controllerPort = Integer.parseInt(args[2]);
			int port = Integer.parseInt(args[3]);

			new Thread(
				new ChunkServer(storageDirectory, controllerHostName, controllerPort, port)
			).start();
		} catch(Exception e) {
			System.out.println("Usage: ChunkServer storageDirectory controllerHostName controllerPort port");
			System.exit(1);
		}
	}

	@Override
	public void run() {
		try {
			serverSocket = new ServerSocket(port);
			LOGGER.info("Chunk server started successfully");

			//start HeartbeatTask that executes every 30 seconds
			Timer timer = new Timer();
			timer.schedule(new HeartbeatTask(), 0, 30 * 1000);

			while(!stopped) {
				Socket socket = serverSocket.accept();
				LOGGER.fine("Received connection from '" + socket.getInetAddress() + ":" + socket.getPort() + "'.");
			
				new Thread(new ChunkServerWorker(socket, this)).start();
			}
		} catch(Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	public synchronized void writeChunk(String filename, int chunkNum, int length, byte[] bytes, boolean eof, long timestamp) throws Exception{
		//search for filename
		List<Integer> chunkList;
		if(chunks.containsKey(filename)) {
			chunkList = chunks.get(filename);
		} else {
			chunkList = new LinkedList<Integer>();
			chunks.put(filename, chunkList);
		}

		//search for chunk number
		int version = 1;
		if(chunkList.contains(chunkNum)) {
			//check timestamp in file and increase version number and rewrite if need be
			try {
				File file = new File(storageDirectory + filename + "_chunk" + chunkNum);
				DataInputStream in = new DataInputStream(new FileInputStream(file));

				version = in.readInt();
				long chunkTimestamp = in.readLong();
				in.close();

				if(chunkTimestamp < timestamp) {
					version++;
					file.delete();
				} else {
					return;
				}
			} catch(Exception e) {
				return;
			}
		} else {
			chunkList.add(chunkNum);
		}

		//add to newChunks for minor heartbeat update
		if(newChunks.containsKey(filename)) {
			chunkList = newChunks.get(filename);
		} else {
			chunkList = new LinkedList<Integer>();
			newChunks.put(filename, chunkList);
		}

		chunkList.add(chunkNum);

		//create new file if needed
		File file = new File(storageDirectory + filename + "_chunk" + chunkNum);
		file.getParentFile().mkdirs();
		DataOutputStream out = new DataOutputStream(new FileOutputStream(file));
		
		//write out metadata
		out.writeInt(version);
		out.writeLong(timestamp);
		out.writeInt(chunkNum);
		out.writeInt(length);
		out.writeBoolean(eof);

		//write bytes and interlacing SHA1 digests
		MessageDigest messageDigest = MessageDigest.getInstance("SHA1");
		int i=0;
		for(i=0; i<length; i++) {
			out.writeByte(bytes[i]);
			messageDigest.update(bytes[i]);

			if(i % DIGEST_LENGTH == 0) {
				for(byte b : messageDigest.digest()) {
					out.writeByte(b);
				}

				messageDigest.reset();
			}
		}

		if(i % DIGEST_LENGTH != 0) {
			for(byte b : messageDigest.digest()) {
				out.writeByte(b);
			}
		}

		LOGGER.info("Wrote chunk '" + filename + ":" + chunkNum + "' - length:" + length + " eof:" + eof + " timestamp:" + timestamp);
		out.close();
	}

	public synchronized Message requestChunk(String filename, int chunkNum) throws Exception {
		if(!chunks.containsKey(filename) || (!chunks.get(filename).contains(chunkNum))) {
			throw new Exception("Chunk server doesn't contain chunk '" + filename + ":" + chunkNum + "'");
		}

		//read in metadata
		try {
			DataInputStream in = new DataInputStream(new FileInputStream(storageDirectory + filename + "_chunk" + chunkNum));
			int version = in.readInt();
			long timestamp = in.readLong();
			int chunkNumber = in.readInt();
			int length = in.readInt();
			boolean eof = in.readBoolean();

			//read bytes and check SHA1 digests
			byte[] bytes = new byte[length];
			MessageDigest messageDigest = MessageDigest.getInstance("SHA1");
			int i=0;
			for(i=0; i<length; i++) {
				bytes[i] = in.readByte();
				messageDigest.update(bytes[i]);

				if(i % DIGEST_LENGTH == 0) {
					byte[] digest = messageDigest.digest();
					for(int j=0; j<digest.length; j++) {
						if(in.readByte() != digest[j]) {
							//start chunk fix thread
							new Thread(new ChunkServerFixChunk(filename, chunkNum)).start();
							throw new Exception("Data corruption detected in chunk '" + filename + ":" + chunkNum + "'");
						}
					}

					messageDigest.reset();
				}
			}

			if(i % DIGEST_LENGTH != 0) {
				byte[] digest = messageDigest.digest();
				for(int j=0; j<digest.length; j++) {
					if(in.readByte() != digest[j]) {
						//start chunk fix thread
						new Thread(new ChunkServerFixChunk(filename, chunkNum)).start();
						throw new Exception("Data corruption detected in chunk '" + filename + ":" + chunkNum + "'");
					}
				}

				messageDigest.reset();
			}

			//if there's anymore data in the file there's data corruption
			try {
				in.readByte();

				//start chunk fix thread
				new Thread(new ChunkServerFixChunk(filename, chunkNum)).start();
				throw new Exception("Data corruption detected in chunk '" + filename + ":" + chunkNum + "'");
			} catch(Exception e) {}

			in.close();
			return new ReplyChunkMsg(filename, chunkNum, length, bytes, eof, timestamp);
		} catch(EOFException e) {
			//start chunk fix thread
			new Thread(new ChunkServerFixChunk(filename, chunkNum)).start();
			throw new Exception("Data corruption detected in chunk '" + filename + ":" + chunkNum + "'");
		}
	}

	public synchronized void stop() {
		stopped = true;
	}

	public boolean getStopped() {
		return stopped;
	}

	class ChunkServerFixChunk extends Thread {
		private String filename;
		private int chunkNum;

		public ChunkServerFixChunk(String filename, int chunkNum) {
			this.filename = filename;
			this.chunkNum = chunkNum;
		}

		@Override
		public void run() {
			try {
				//delete chunk locally
				List<Integer> chunkList = chunks.get(filename);
				int index = chunkList.indexOf(chunkNum);
				if(index < 0) {
					return;
				}

				chunkList.remove(index);
				if (chunkList.size() == 0) {
					chunks.remove(filename);
				}

				File file = new File(storageDirectory + filename + "_chunk" + chunkNum);
				file.delete();

				//send data corruption message to controller
				Socket socket = new Socket(controllerHostName, controllerPort);

				DataCorruptionMsg dcMsg = new DataCorruptionMsg(filename, chunkNum, InetAddress.getLocalHost(), port);
				ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
				out.writeObject(dcMsg);

				socket.close();
			} catch(Exception e) {
				e.printStackTrace();
			}
		}
	}

	class HeartbeatTask extends TimerTask {
		private int count;

		public HeartbeatTask() {
			count = 0;
		}

		@Override
		public void run() {
			try {
				//count total chunks
				int totalChunks = 0;
				for(List<Integer> list : chunks.values()) {
					totalChunks += list.size();
				}

				//determine free space
				long freeSpace = Long.MAX_VALUE - totalChunks;

				//create heartbeat message
				ChunkServerHeartbeatMsg message = new ChunkServerHeartbeatMsg(new ChunkServerMetadata(serverSocket.getInetAddress(), serverSocket.getLocalPort(), totalChunks, freeSpace));
				if(count % 8 == 0) { //8 * 30 seconds = major every 5 minutes
					LOGGER.fine("Executing major HeartbeatTask");
					message.setChunks(chunks);
				} else {
					LOGGER.fine("Executing minor HeartbeatTask");
					message.setChunks(newChunks);
				}

				//send heartbeat message
				Socket socket = new Socket(controllerHostName, controllerPort);
				ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
				out.writeObject(message);

				newChunks.clear();
				socket.close();
			} catch(Exception e) {
				e.printStackTrace();
			}

			count++;
		}
	}
}
