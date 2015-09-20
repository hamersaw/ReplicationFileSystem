package com.hamersaw.replication_file_system;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import java.lang.NumberFormatException;

import java.net.Socket;

import java.util.List;
import java.util.logging.Logger;
import java.util.Scanner;

import com.hamersaw.replication_file_system.message.ErrorMsg;
import com.hamersaw.replication_file_system.message.Message;
import com.hamersaw.replication_file_system.message.RequestChunkMsg;
import com.hamersaw.replication_file_system.message.RequestChunkServerMsg;
import com.hamersaw.replication_file_system.message.ReplyChunkMsg;
import com.hamersaw.replication_file_system.message.ReplyChunkServerMsg;
import com.hamersaw.replication_file_system.message.WriteChunkMsg;

public class Client {
	private static final Logger LOGGER = Logger.getLogger(Client.class.getCanonicalName());
	public static final int CHUNK_SIZE = 65536;

	public static void main(String[] args) {
		String controllerHostName = null;
		int controllerPort = 0;

		try {
			controllerHostName = args[0];
			controllerPort = Integer.parseInt(args[1]);
		} catch(Exception e) {
			System.out.println("Usage: Client controllerHosName controllerPort");
			System.exit(1);
		}

		try {
			//start input loop
			String input = "";
			Scanner scanner = new Scanner(System.in);
			while(!input.equalsIgnoreCase("q")) {
				System.out.print("Options\nS) Store File\nR) Retrieve File\nQ) Quit\nInput:");
				input = scanner.nextLine();

				if(input.equalsIgnoreCase("s")) {
					//read in filename
					String filename;
					FileInputStream in;
					long timestamp, fileLength;
					try {
						System.out.print("\tFilename:");
						filename = scanner.nextLine();
						File file = new File(filename);
						timestamp = file.lastModified();
						fileLength = file.length();
						in = new FileInputStream(file);
					} catch(FileNotFoundException e) {
						System.out.println("File not found.");
						continue;
					} catch(Exception e) {
						e.printStackTrace();
						continue;
					}

					//loop through file ever 64KB
					byte[] bytes = new byte[CHUNK_SIZE];
					int length, chunkNum = 0;
					while((length = in.read(bytes)) != -1) {
						//write chunk server request message
						Socket socket = new Socket(controllerHostName, controllerPort);
						ObjectOutputStream socketOut = new ObjectOutputStream(socket.getOutputStream());
						RequestChunkServerMsg rcsMsg = new RequestChunkServerMsg(filename, chunkNum, true);
						socketOut.writeObject(rcsMsg);

						//read chunk server reply message
						ObjectInputStream socketIn = new ObjectInputStream(socket.getInputStream());
						Message replyMsg = (Message) socketIn.readObject();
						socket.close();

						if(replyMsg.getMsgType() == Message.ERROR_MSG) {
							LOGGER.severe(((ErrorMsg)replyMsg).getMsg());
							return;
						} else if(replyMsg.getMsgType() != Message.REPLY_CHUNK_SERVER_MSG) {
							LOGGER.severe("Unexpected message type returned. Was expecting '" + Message.REPLY_CHUNK_SERVER_MSG + "' and recieved '" + replyMsg.getMsgType() + "'");
							return;
						}

						//figure out if this is the end of the file
						boolean eof;
						if(CHUNK_SIZE * chunkNum + length == fileLength) {
							eof = true;
						} else {
							eof = false;
						}

						//send chunk payload message
						List<ChunkServerMetadata> chunkServers = ((ReplyChunkServerMsg) replyMsg).getChunkServers();
						ChunkServerMetadata chunkServerMetadata = chunkServers.get(0);
						chunkServers.remove(0);
						WriteChunkMsg wcMsg = new WriteChunkMsg(filename, chunkNum, length, bytes, eof, timestamp, chunkServers);

						Socket chunkServerSocket = new Socket(chunkServerMetadata.getInetAddress(), chunkServerMetadata.getPort());
						ObjectOutputStream chunkServerOut = new ObjectOutputStream(chunkServerSocket.getOutputStream());
						chunkServerOut.writeObject(wcMsg);
						chunkServerSocket.close();

						chunkNum++;
					}

					in.close();
				} else if(input.equalsIgnoreCase("r")) {
					//read in filename
					String filename;
					FileInputStream in;
					FileOutputStream out;
					try {
						System.out.print("\tFilename:");
						filename = scanner.nextLine();
						in = new FileInputStream(new File(filename));

						System.out.print("\tOutputDirectory:");
						File file = new File(scanner.nextLine() + filename);
						file.getParentFile().mkdirs();
						out = new FileOutputStream(file);
					} catch(FileNotFoundException e) {
						System.out.println("File not found.");
						continue;
					} catch(Exception e) {
						e.printStackTrace();
						continue;
					}

					boolean eof = false;
					int chunkNum = 0;
					while(!eof) {
						//write request chunk servers for filename
						Socket socket = new Socket(controllerHostName, controllerPort);
						ObjectOutputStream socketOut = new ObjectOutputStream(socket.getOutputStream());
						RequestChunkServerMsg rcsMsg = new RequestChunkServerMsg(filename, chunkNum, false);
						socketOut.writeObject(rcsMsg);

						//read request chunk servers
						ObjectInputStream socketIn = new ObjectInputStream(socket.getInputStream());
						Message replyMsg = (Message) socketIn.readObject();
						socket.close();

						if(replyMsg.getMsgType() == Message.ERROR_MSG) {
							LOGGER.severe(((ErrorMsg)replyMsg).getMsg());
							return;
						} else if(replyMsg.getMsgType() != Message.REPLY_CHUNK_SERVER_MSG) {
							LOGGER.severe("Unexpected message type returned. Was expecting '" + Message.REPLY_CHUNK_SERVER_MSG + "' and recieved '" + replyMsg.getMsgType() + "'");
							return;
						}

						//request chunks from chunk servers
						List<ChunkServerMetadata> chunkServers = ((ReplyChunkServerMsg) replyMsg).getChunkServers();
						RequestChunkMsg rcMsg = new RequestChunkMsg(filename, chunkNum);
						boolean success = false;
						for(int i=0; i<chunkServers.size(); i++) {
							Socket clientSocket = new Socket(chunkServers.get(i).getInetAddress(), chunkServers.get(i).getPort());
							ObjectOutputStream clientOut = new ObjectOutputStream(clientSocket.getOutputStream());
							clientOut.writeObject(rcMsg);

							ObjectInputStream clientIn = new ObjectInputStream(clientSocket.getInputStream());
							replyMsg = (Message) clientIn.readObject();
							clientSocket.close();

							if(replyMsg.getMsgType() == Message.ERROR_MSG) {
								LOGGER.severe(((ErrorMsg)replyMsg).getMsg());
								continue;
							} else if(replyMsg.getMsgType() != Message.REPLY_CHUNK_MSG) {
								LOGGER.severe("Unexpected message type returned. Was expecting '" + Message.REPLY_CHUNK_MSG + "' and recieved '" + replyMsg.getMsgType() + "'");
								continue;
							}

							//write chunk out to file
							byte[] bytes = ((ReplyChunkMsg) replyMsg).getBytes();
							LOGGER.info("Chunk '" + filename + ":" + chunkNum + "' has length '" + bytes.length + "'");
							out.write(bytes);

							eof = ((ReplyChunkMsg) replyMsg).getEof();
							success = true;
							break;
						}

						if(!success) {
							LOGGER.severe("Unable to retreive file '" + filename + "'");
							return;
						}

						chunkNum++;
					}

					out.close();
				} else if(!input.equalsIgnoreCase("q")) {
					System.out.println("Unknown input '" + input  + "' please reenter.");
				}
			}
		} catch(Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
}
