import java.io.*;
import java.net.*;
import java.nio.file.Files;
import java.util.*;
import java.util.Arrays;
import java.io.ObjectOutputStream;


public class fileOwner {
     static String chunksDirectory = "./temp";
     static int numberofchunks = 0;
     static int max_clients = 5; //total clients we will have
     static int sPort;

    public static void main(String[] args) throws Exception {

        try {
            if (args.length < 1) {
                System.out.println("Usage: java fileOwner [ServerPort].");
                return;
            }
            sPort = Integer.parseInt(args[0]);
            System.out.println("Server Listening at Port:" + Integer.parseInt(args[0]));
            fileOwner FO = new fileOwner();
            FO.splitFile(new File("./test.pdf")); //split file for test.pdf
            System.out.println("Split test.pdf...");
            File[] chunkfiles = new File(chunksDirectory).listFiles();

            //sort files by name order 1, 2, 3, 4 ...etc
            Arrays.sort(chunkfiles, (f1, f2) -> {
                int fileId1 = Integer.parseInt(f1.getName());
                int fileId2 = Integer.parseInt(f2.getName());
                return fileId1 - fileId2;
            });

            //for DEBUG
            //for(int i = 0 ; i<40 ; i++){
            //    System.out.print(chunkfiles[i].getName()+ ", ");
            //}

            numberofchunks = chunkfiles.length;
            System.out.println("Number of Chunks: " + numberofchunks);
            System.out.println("Chunk directory:" + chunksDirectory);

            //now we split the total chunk files to clients evenly, and form a table to memorize them
            //construct chunk table list
            //it will assign which chunk goes to which peer, sometimes the last peer gets extra chunks
            Map<Integer, ArrayList<Integer>> clinet_chunk_table = new LinkedHashMap<Integer, ArrayList<Integer>>();
            int seperate_chunks = numberofchunks / max_clients;
            int chunk_num, j;
            for (int i = 1; i <= max_clients; i++) {
                ArrayList<Integer> arr = new ArrayList<Integer>();
                if (i == 1)
                    chunk_num = 1;
                else
                    chunk_num = (i - 1) * seperate_chunks + 1;
                for (j = chunk_num; j <= seperate_chunks + chunk_num - 1; j++) {
                    arr.add(j);
                }
                if (j <= numberofchunks && i == max_clients) {
                    for (; j <= numberofchunks; j++)
                        arr.add(j);
                }
                clinet_chunk_table.put(i, arr);
            }
            System.out.println("Show the server chunk table below: ");
            System.out.println(clinet_chunk_table);

            if (numberofchunks > 0) {
                //listen at the server port
                System.out.println("The server is running, and listening on port: " + sPort);
                ServerSocket listener = new ServerSocket(sPort);

                try {
                    int clientNum = 1;
                    while (true) {
                        if (clientNum <= max_clients) {
                            new ServerHandler(listener.accept(), clientNum, chunkfiles, clinet_chunk_table.get(clientNum)).start();
                            System.out.println("Client " + clientNum + " is connected!");
                            clientNum++;
                        } else {
                            System.out.println("Exceed max_clients that server can serve... terminate the server.");
                            break;
                        }
                    }
                } finally {
                    listener.close();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }



    public  void splitFile(File f) throws IOException {
        int partCounter = 1;
        byte[] buffer = new byte[100000]; //100K

        try (FileInputStream fis = new FileInputStream(f);
             BufferedInputStream bis = new BufferedInputStream(fis)) {


            //create temp dir
            File newDir = new File("./temp");
            newDir.mkdir();
            //read the target file, and store it according to the buffer inorder
            int bytesAmount = 0;
            while ((bytesAmount = bis.read(buffer)) > 0) {
                //write each chunk of data into separate file with different numbers
                String filePartName = String.format("./temp/%d", partCounter++);
                File newFile = new File(f.getParent(), filePartName);
                try (FileOutputStream out = new FileOutputStream(newFile)) {
                    out.write(buffer, 0, bytesAmount);
                }
            }
        }
    }//end of splitFile

    /*test for merging chunks
    public static void mergeFiles(List<File> files, File into)
            throws IOException {
        try (FileOutputStream fos = new FileOutputStream(into);
             BufferedOutputStream mergingStream = new BufferedOutputStream(fos)) {
            for (File f : files) {
                Files.copy(f.toPath(), mergingStream);
            }
        }
    }*/
} //end of fileOwner class


//After the client connect with server, the server create a thread for that client, and start to send
//that client chunks according to its server chunk table
class ServerHandler extends Thread {
    Socket connection;
    int clientNum;
    File[] chunkfiles;
    ArrayList<Integer> chunk_table;
    private ObjectOutputStream out;    //stream write to the socket

    //constructor
    public ServerHandler(Socket connection, int clientNum, File[] chunkfiles, ArrayList<Integer> chunk_table) {
        this.connection = connection;
        this.clientNum = clientNum;
        this.chunkfiles = chunkfiles;
        this.chunk_table = chunk_table;
    }

    public void run() {
        try {
            System.out.println("Chunk table assigned to client " +clientNum+ " :" + chunk_table );
            byte[] data_buffer = new byte[100000]; //100K
            //now we have to send those files belongs to the client in the chunk table
            //get the output stream first
            out = new ObjectOutputStream(connection.getOutputStream());
            out.flush();

            //first tell each client the total files server has split
            out.writeObject(chunkfiles.length);

            //second we tell each client their specific files, according to the chunk_table
            //how many files that server are going to send to the connected client
            out.writeObject(chunk_table.size());

            //Now we send each chunk file
            for(int i = 0 ; i < chunk_table.size() ; i++) {
                //send the chunk file's name
                out.writeObject(chunkfiles[chunk_table.get(i)-1].getName());

                //send the chunk data
                //read the data into data_buffer from that file
                FileInputStream fin = new FileInputStream(chunkfiles[chunk_table.get(i)-1]);
                BufferedInputStream bin = new BufferedInputStream(fin);
                int data_size = bin.read(data_buffer);
                System.out.println("Send chunk " + chunkfiles[chunk_table.get(i)-1].getName() + " to client " + clientNum + ".");

                //send data size
                out.writeObject(data_size);
                //send chunk data
                out.writeObject(data_buffer);

                //[DEBUG] Because we continuously writeObject for data_buffer, same byte array here in the loop
                //so we have to reset the OutputObjectStream, otherwise, it won't send anything in case of resending
                //the same object too many times
                out.reset();

                bin.close();
                fin.close();
                Thread.sleep(100);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try{
                out.flush();
                out.close();
                connection.close();
                System.out.println("Done sending chunks to client " + clientNum + " , close the connection.");
                //System.out.println("Server disconnect with client " + clientNum + " .");
            }
            catch(Exception e){
                e.printStackTrace();
            }

        }
    }

}