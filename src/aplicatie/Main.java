
import static java.awt.SystemColor.text;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.rmi.AccessException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class Main {

    static int regPort = Configurations.REG_PORT;
    BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
    static Registry registry;

    /**
     * respawns replica servers and register replicas at master
     *
     * @param master
     * @throws IOException
     */
    static void respawnReplicaServers(Master master) throws IOException {
        System.out.println("[@main] respawning replica servers ");
        // TODO make file names global
        BufferedReader br = new BufferedReader(new FileReader("repServers.txt"));
        int n = Integer.parseInt(br.readLine().trim());
        ReplicaLoc replicaLoc;
        String s;

        for (int i = 0; i < n; i++) {
            s = br.readLine().trim();
            replicaLoc = new ReplicaLoc(i, s.substring(0, s.indexOf(':')), true);
            ReplicaServer rs = new ReplicaServer(i, "./");

            ReplicaInterface stub = (ReplicaInterface) UnicastRemoteObject.exportObject(rs, 0);
            registry.rebind("ReplicaClient" + i, stub);

            master.registerReplicaServer(replicaLoc, stub);

            System.out.println("replica server state [@ main] = " + rs.isAlive());
        }
        br.close();
    }

    public static void launchClients() throws IOException, NotBoundException, MessageNotFoundException {

        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

        System.out.println("Comenziile disponibile sunt:");
        System.out.println("dir type del write");

        String nume;
        String filename;
        int x;
        boolean k = true;
        while (k == true) {
            String text;

            char[] scriere;
            byte[] data;

            nume = reader.readLine();
            if (nume.equalsIgnoreCase("dir")) {
                x = 1; //afisare
            } else if (nume.equalsIgnoreCase("type")) {
                x = 2;//citire
            } else if (nume.equalsIgnoreCase("del")) {
                x = 3;//stergere
            } else if (nume.equalsIgnoreCase("write")) {
                x = 4;//scriere
            } else {
                x = 0;
            }

            switch (x) {

                case 1:
                    
                    File directoryPath = new File("C:\\Users\\Rainy\\OneDrive\\Desktop\\Proiect\\Replica_1");
                    String[] continut = directoryPath.list(); // tablou cu fisierele din folderul Replica_1
                    System.out.println("Numarul de fisiere aflate in folder: " + continut.length);
                    System.out.println("Se vor afisa fisierele si directoarele din folder: ");
                    for (int i = 0; i < continut.length; i++) {
                        System.out.println(continut[i]);
                    }  //afiseaza numele fisierelor
                    break;

                case 2:
                    Client client1 = new Client();// crearea unui client nou

                    System.out.println("Comanda selectata-> type: Numele fisierului citit");
                    System.out.println();
                    String file = reader.readLine();//se citeste de la tastatura
                    scriere = " ".toCharArray();// scriu un spatiu ca sa pot citi fisierul
                    data = new byte[scriere.length];
                    for (int i = 0; i < scriere.length; i++) {
                        data[i] = (byte) scriere[i];
                    }
                    client1.write(file, data);

                    String afiseaza = new String(client1.read(file));// variabila de tip string pentru afisarea continutului

                    System.out.println("Continut: ");
                    System.out.println(afiseaza);// afisare continut fisier

                    break;

                case 3:

                    

                    
                    System.out.println("Comanda selectata->del : Introdu numele fisierului pe care il stergi ");
                    System.out.println();
                    String nume_fisier = "";
                    String fisierdel = reader.readLine(); // numele fisierului de sters

                    nume_fisier = fisierdel;
                    for (int s = 0; s < 3; s++) {
                        String fileName = "C:\\Users\\Rainy\\OneDrive\\Desktop\\Proiect\\Replica_" + x + "\\" + fisierdel;
                        try {
                            Files.deleteIfExists(Paths.get(fileName));
                            System.out.println("Fisier sters");

                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }

                    break;

                case 4:
                    Client client3 = new Client();
                    System.out.println("Comanda selectata-> write : Fisierul in care doriti sa scrieti ");
                    System.out.println();
                    filename = reader.readLine();//citim de la tastatura
                    System.out.println("Introduceti Textul: ");
                    System.out.println();
                    text = reader.readLine();
                    scriere = text.toCharArray();
                    data = new byte[scriere.length];
                    for (int i = 0; i < scriere.length; i++) {
                        data[i] = (byte) scriere[i];
                    }

                    client3.write(filename, data);

                    break;

                default:

                    System.out.println("EXIT");
                    System.exit(0);

            }

        }

    }

    /**
     * runs a custom test as follows 1. write initial text to "file1" 2. reads
     * the recently text written to "file1" 3. writes a new message to "file1"
     * 4. while the writing operation in progress read the content of "file1" 5.
     * the read content should be = to the initial message 6. commit the 2nd
     * write operation 7. read the content of "file1", should be = initial
     * messages then second message
     *
     * @throws IOException
     * @throws NotBoundException
     * @throws MessageNotFoundException
     */
    public static void customTest() throws IOException, NotBoundException, MessageNotFoundException {
        Client c = new Client();
        String fileName = "file1";

        char[] ss = "[INITIAL DATA!]".toCharArray(); // len = 15
        byte[] data = new byte[ss.length];
        for (int i = 0; i < ss.length; i++) {
            data[i] = (byte) ss[i];
        }

        c.write(fileName, data);

        c = new Client();
        ss = "File 1 test test END".toCharArray(); // len = 20
        data = new byte[ss.length];
        for (int i = 0; i < ss.length; i++) {
            data[i] = (byte) ss[i];
        }

        byte[] chunk = new byte[Configurations.CHUNK_SIZE];

        int seqN = data.length / Configurations.CHUNK_SIZE;
        int lastChunkLen = Configurations.CHUNK_SIZE;

        if (data.length % Configurations.CHUNK_SIZE > 0) {
            lastChunkLen = data.length % Configurations.CHUNK_SIZE;
            seqN++;
        }

        WriteAck ackMsg = c.masterStub.write(fileName);
        ReplicaServerClientInterface stub = (ReplicaServerClientInterface) registry.lookup("ReplicaClient" + ackMsg.getLoc().getId());

        FileContent fileContent;
        @SuppressWarnings("unused")
        ChunkAck chunkAck;
        //		for (int i = 0; i < seqN; i++) {
        System.arraycopy(data, 0 * Configurations.CHUNK_SIZE, chunk, 0, Configurations.CHUNK_SIZE);
        fileContent = new FileContent(fileName, chunk);
        chunkAck = stub.write(ackMsg.getTransactionId(), 0, fileContent);

        System.arraycopy(data, 1 * Configurations.CHUNK_SIZE, chunk, 0, Configurations.CHUNK_SIZE);
        fileContent = new FileContent(fileName, chunk);
        chunkAck = stub.write(ackMsg.getTransactionId(), 1, fileContent);

        // read here 
        List<ReplicaLoc> locations = c.masterStub.read(fileName);
        System.err.println("[@CustomTest] Read1 started ");

        // TODO fetch from all and verify 
        ReplicaLoc replicaLoc = locations.get(0);
        ReplicaServerClientInterface replicaStub = (ReplicaServerClientInterface) registry.lookup("ReplicaClient" + replicaLoc.getId());
        fileContent = replicaStub.read(fileName);
        System.err.println("[@CustomTest] data:");
        System.err.println(new String(fileContent.getData()));

        // continue write 
        for (int i = 2; i < seqN - 1; i++) {
            System.arraycopy(data, i * Configurations.CHUNK_SIZE, chunk, 0, Configurations.CHUNK_SIZE);
            fileContent = new FileContent(fileName, chunk);
            chunkAck = stub.write(ackMsg.getTransactionId(), i, fileContent);
        }
        // copy the last chuck that might be < CHUNK_SIZE
        System.arraycopy(data, (seqN - 1) * Configurations.CHUNK_SIZE, chunk, 0, lastChunkLen);
        fileContent = new FileContent(fileName, chunk);
        chunkAck = stub.write(ackMsg.getTransactionId(), seqN - 1, fileContent);

        //commit
        ReplicaLoc primaryLoc = c.masterStub.locatePrimaryReplica(fileName);
        ReplicaServerClientInterface primaryStub = (ReplicaServerClientInterface) registry.lookup("ReplicaClient" + primaryLoc.getId());
        primaryStub.commit(ackMsg.getTransactionId(), seqN);

        // read
        locations = c.masterStub.read(fileName);
        System.err.println("[@CustomTest] Read3 started ");

        replicaLoc = locations.get(0);
        replicaStub = (ReplicaServerClientInterface) registry.lookup("ReplicaClient" + replicaLoc.getId());
        fileContent = replicaStub.read(fileName);
        System.err.println("[@CustomTest] data:");
        System.err.println(new String(fileContent.getData()));

    }

    static Master startMaster() throws AccessException, RemoteException {
        Master master = new Master();
        MasterServerClientInterface stub
                = (MasterServerClientInterface) UnicastRemoteObject.exportObject(master, 0);
        registry.rebind("MasterServerClientInterface", stub);
        System.err.println("Server ready");
        return master;
    }

    public static void main(String[] args) throws IOException, NotBoundException, MessageNotFoundException {

        try {
            LocateRegistry.createRegistry(regPort);
            registry = LocateRegistry.getRegistry(regPort);

            Master master = startMaster();
            respawnReplicaServers(master);

//			customTest();
            launchClients();

        } catch (RemoteException e) {
            System.err.println("Server exception: " + e.toString());
            e.printStackTrace();
        }
    }

}
