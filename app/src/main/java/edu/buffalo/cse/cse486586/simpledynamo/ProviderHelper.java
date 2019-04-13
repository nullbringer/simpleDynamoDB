package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.Context;
import android.content.SharedPreferences;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.LinkedHashSet;
import java.util.TreeMap;

public class ProviderHelper {


    private Context context;

    public ProviderHelper(Context context) {
        this.context = context;
    }

    /*
     * Establish connecton to another node and write send a String
     * */

    public Socket connectAndWriteMessege(int thisPort, String msg) throws IOException {

        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                thisPort);

        socket.setSoTimeout(Constants.SOCKET_READ_TIMEOUT);

        DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
        dataOutputStream.writeUTF(msg);
        dataOutputStream.flush();

        return socket;

    }

    public String readAckAndClose(Socket socket) throws IOException {

        DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());

        String reply = dataInputStream.readUTF();

        /*
         * Received data from successor!
         * */


        dataInputStream.close();
        return reply;

    }

    /* Writes standard ACK in open channel */

    public void returnStandardAcknoldegement(Socket clientSocket) throws IOException{

        DataOutputStream dataOutputStream = new DataOutputStream(clientSocket.getOutputStream());
        dataOutputStream.writeUTF(Constants.ACK_VALUE);
        dataOutputStream.flush();

        dataOutputStream.close();

    }

    public void saveKeyPairInDataStore(Message message){

        SharedPreferences sharedPref = context.getSharedPreferences(Constants.PREFERENCE_FILE, Context.MODE_PRIVATE);
        SharedPreferences.Editor editor = sharedPref.edit();

        editor.putString(message.getKey(), message.getValue());
        editor.apply();
    }

    public LinkedHashSet<String> getTargetNodesForKey(Message message, TreeMap<String, String> ringStructure) throws NoSuchAlgorithmException {

        String targetNode = getNextNode(message.getKey(),ringStructure);
        String replicationNode1 = getNextNode(targetNode,ringStructure);
        String replicationNode2 = getNextNode(replicationNode1,ringStructure);

        LinkedHashSet<String> targetNodes = new LinkedHashSet<String>();

        targetNodes.add(targetNode);
        targetNodes.add(replicationNode1);
        targetNodes.add(replicationNode2);

        return targetNodes;
    }

    private String getNextNode(String key, TreeMap<String, String> ringStructure) throws NoSuchAlgorithmException{

        String hashedKey = genHash(key);

        String targetHash  = ringStructure.higherKey(hashedKey);

        if(targetHash == null){
            targetHash = ringStructure.firstKey();
        }

        return ringStructure.get(targetHash);

    }


    public String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }



}
