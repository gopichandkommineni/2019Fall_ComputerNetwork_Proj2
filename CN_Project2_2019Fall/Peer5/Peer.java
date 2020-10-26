import java.io.*;
import java.net.*;
import java.nio.file.Files;
import java.util.*;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


public class Peer {
	private int sPort, pPort, nPort;
	Socket requestSocket;           //socket connect to the server
	Socket neighborSocket;
	ObjectInputStream in;          //stream read from the socket
	ObjectOutputStream out;          //stream read from the socket
	Set<Integer> checkList;
	Lock lock = new ReentrantLock();
	//main method
	public static void main(String args[]) throws Exception {

		try{
			Peer p = new Peer();
			if (args.length < 3) {
				System.out.println("Usage: java Peer [ServerPort] [PeerPort] [NeighborPort].");
				return;
			}

			p.sPort = Integer.parseInt(args[0]);
			p.pPort = Integer.parseInt(args[1]);
			p.nPort = Integer.parseInt(args[2]);
			System.out.println("Start listening on Port: " + p.pPort);
			System.out.println("Neighbor is on port: " + p.nPort);

			//create temp dir
			File newDir = new File("./temp");
			newDir.mkdir();

			//now we connect to fileOwner, which is the server
			p.requestSocket = new Socket("127.0.0.1", p.sPort);
			System.out.println("Successfully connect to fileOwner on port: " + p.sPort);

			//get input stream
			p.in = new ObjectInputStream(p.requestSocket.getInputStream());

			//now we could start to get the input from fileOwner, get the chunk files!!!
			//first we get to know how many chunk files we need to assemble the org test.pdf
			int totalFiles = (int) p.in.readObject();
			p.checkList = new HashSet<Integer>();
			//System.out.print("[DEBUG] total Files="+totalFiles);
			for(int i = 1 ; i <= totalFiles ; i++){
				//then we establish a checklist for these files, if we get them all, we could start to assemble all the chunks
				p.checkList.add(i);
			}
			System.out.println("Total chunk files needed: " + totalFiles + ", list: " + p.checkList);

			System.out.println("Starts to receive chunks from fileOwner...");
			//second we start to receive the files that the server send to peer at the beginning
			//get to number of files
			int numFilesFromServer = (int) p.in.readObject();
			System.out.println("Going to receive " + numFilesFromServer + " chunks from fileOwner...");
			while(numFilesFromServer > 0) {
				//receive the chunk files one by one here
				String fileName = (String) p.in.readObject(); //chunk name
				int data_size = (int) p.in.readObject(); //chunk data size
				byte[] data_buffer = (byte[]) p.in.readObject(); //chunk data

				String filePartName = String.format("./temp/%d", Integer.parseInt(fileName));
				File newFile = new File(filePartName);

				FileOutputStream fout = new FileOutputStream(newFile);
				BufferedOutputStream bout = new BufferedOutputStream(fout);
				bout.write(data_buffer, 0, data_size);
				//System.out.println("[DEBUG] Data_size = " + data_size + ", Filename = " + filePartName);

				System.out.println("Receive chunk " + fileName + " from fileOwner.");
				p.checkList.remove(Integer.parseInt(fileName));
				System.out.println("Update peer checklist: " + p.checkList);
				bout.flush();
				bout.close();
				fout.close();
				numFilesFromServer--;
			}
			p.in.close();
			p.requestSocket.close();
			System.out.println("Finish receiving chunks from fileOwner. Disconnect with fileOwner.");

			//We finished receiving files from the fileOwner, now we still miss some chunks...
			//First, we will establish a thread, to be file sharer, share to other peers
			//Second, we will keep asking for missing chunks from our neighbor...
			//be the file sharer (server) here
			new PeerAsServer(p.pPort, p.lock).start();


			//start to asking missing chunks from our upload neighbor
			//keep polling our neighbor until connection established
			boolean not_connected = true;
			while(not_connected) {
				try {
					p.neighborSocket = new Socket("127.0.0.1", p.nPort);
					System.out.println("Connected to our neighbor at port: " + p.nPort);
					p.in = new ObjectInputStream(p.neighborSocket.getInputStream());
					p.out = new ObjectOutputStream(p.neighborSocket.getOutputStream());
					p.out.flush();
					not_connected = false;
				} catch(ConnectException e) {
					System.out.println("Failed to connect neighbor on port: " + p.nPort + "... Retrying...");
					Thread.sleep(3000);
					not_connected = true;
				}
			}

			//after we connected to our neighbor, we start asking the missing chunk files
			while(true) {
				System.out.println("Chunk list that going to ask for: " + p.checkList);
				if(!p.checkList.isEmpty()) {
					//convert the checklist to int array
					Integer[] fileName = p.checkList.toArray(new Integer[p.checkList.size()]);
					for(int i = 0 ; i < fileName.length ; i++) {
						//send the fileName we want to our neighbor
						System.out.println("Asking for chunk: " + fileName[i]);
						p.out.writeObject(fileName[i]);

						//check if our neighbor has that chunk file
						int data_size = (int) p.in.readObject();
						if(data_size != 0) {
							//add lock??
							//our neighbor has the file we want, get it!
							p.lock.lock(); //lock it while getting new chunk
							byte[] data_buffer = (byte[]) p.in.readObject();

							String filePartName = String.format("./temp/%d", fileName[i]);
							File newFile = new File(filePartName);
							FileOutputStream fout = new FileOutputStream(newFile);
							BufferedOutputStream bout = new BufferedOutputStream(fout);
							bout.write(data_buffer, 0, data_size);
							p.lock.unlock();

							System.out.println("Receive chunk " + fileName[i] + " from neighbor peer.");

							//update chunk list
							p.checkList.remove(fileName[i]);
							System.out.println("Update chunk checklist: " + p.checkList);
							bout.flush();
							bout.close();
						} else {
							System.out.println("Neighbor doesn't have the chunk "+ fileName[i] + ".");
						}

					}
				} else {
					//we get all the chunk files we want
					//inform our neighbor by sending chunk name = 0
					System.out.println(" ");
					System.out.println("Finish receiving all the chunk files!!");
					System.out.println(" ");
					p.out.writeObject(0);
					p.out.flush();
					break;
				}
				Thread.sleep(2000);
			}

			//we finish get all the chunk files, now we will start to assemble them back together
			//get the test.pdf under ./
			System.out.println("Start to merge all the chunk files...");

			//get all the chunk files
			File[] chunk_files = new File("./temp").listFiles();
			//sort them by file name into 1, 2, 4, 5, ... inorder
			Arrays.sort(chunk_files, (f1, f2) -> {
				int fileId1 = Integer.parseInt(f1.getName());
				int fileId2 = Integer.parseInt(f2.getName());
				return fileId1 - fileId2;
			});
			File into = new File("./test.pdf");
			mergeFiles(Arrays.asList(chunk_files), into);

			System.out.println("Finish merging!");
			p.neighborSocket.close();
			p.in.close();
			p.out.close();
			System.out.println("Disconnect all the connections, done.");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	public static void mergeFiles(List<File> files, File into)
			throws IOException {
		try (FileOutputStream fos = new FileOutputStream(into);
			 BufferedOutputStream mergingStream = new BufferedOutputStream(fos)) {
			for (File f : files) {
				Files.copy(f.toPath(), mergingStream);
			}
		}
	}

}

class PeerAsServer extends Thread {
	int pPort;
	Lock lock;
	//constructor
	public PeerAsServer(int pPort, Lock lock) {
		this.pPort = pPort;
		this.lock = lock;
	}

	public void run() {

		try {
			ServerSocket PeerListener = new ServerSocket(pPort);
			System.out.println("Be the upload neighbor, listening on port: " + pPort);
			new PeerServerHandler(PeerListener.accept(), lock).start();
		} catch(Exception e) {
			e.printStackTrace();
		}

	}

}

class PeerServerHandler extends Thread {
	Socket PSconnection;
	ObjectInputStream in;
	ObjectOutputStream out;
	Lock lock;

	public PeerServerHandler(Socket PSconnection, Lock lock){
		this.PSconnection = PSconnection;
		this.lock = lock;
	}

	public void run() {
		try {
			System.out.println("Download neighbor is connected!");

			out = new ObjectOutputStream(PSconnection.getOutputStream());
			out.flush();
			in = new ObjectInputStream(PSconnection.getInputStream());

			byte[] data_buffer = new byte[100000]; //100K
			File chunkDirectory = new File("./temp");

			//after connection established, we will keep read the chunk file request from our neighbor
			//first it will send the file name to here, if we have that file, send it to our neighbor,
			//if we don't have the chunk file that our neighbor wants, we send null value back
			while(true) {
				//keep listening to neighbor's request chunk file name
				int chunkName = (int) in.readObject();
				if(chunkName <= 0)
					break; //our neighbor has already get all the chunks it wants

				System.out.println("Neighbor ask for chunk: " + chunkName);

				//go search the file Name:
				//add lock?
				//make sure we already get that chunk file from the main thread
				lock.lock();
				//get all the chunks that we already had
				File[] chunkfiles = chunkDirectory.listFiles();
				lock.unlock();

				//search chunk
				int fileFound = 0, index = 0;
				for(index = 0 ; index < chunkfiles.length ; index++) {
					//search for the requested chunk file
					if(chunkName == Integer.parseInt(chunkfiles[index].getName())) {
						//the requested chunk file is found
						fileFound = 1;
						break;
					}
				}

				//the requested chunk is found
				if(fileFound == 1) {
					//send the requested chunk to our neighbor
					//send the chunk data
					FileInputStream fin = new FileInputStream(chunkfiles[index]);
					BufferedInputStream bin = new BufferedInputStream(fin);
					int data_size = bin.read(data_buffer);
					System.out.println("Send chunk " + chunkName + " to neighbor peer.");
					out.writeObject(data_size);
					out.writeObject(data_buffer);
					//remember to reset OutputStream in the loop
					out.reset();
					out.flush();
					bin.close();
					fin.close();
				} else {
					//not found, send a data_size = 0, indicate that we don't have that file
					out.writeObject(0);
				}
			}

			//end the Peer-Server thread here
			System.out.println("No more chunk request from the neighbor. Uploader thread terminates.");
			in.close();
			out.close();
			PSconnection.close();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}