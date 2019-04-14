package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.Context;
import android.content.SharedPreferences;
import android.text.TextUtils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Formatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class ProviderHelper {


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

    public void saveKeyPairInDataStore(Message message, Context context){

        SharedPreferences sharedPref = context.getSharedPreferences(Constants.PREFERENCE_FILE, Context.MODE_PRIVATE);
        SharedPreferences.Editor editor = sharedPref.edit();

        editor.putString(message.getKey() + Constants.KEY_VERSION_SEPARATOR + message.getOriginTimestamp(),
                message.getValue());

        editor.apply();
    }

    public void deleteAllLocalData(Context context){

        SharedPreferences sharedPref = context.getSharedPreferences(Constants.PREFERENCE_FILE, Context.MODE_PRIVATE);
        SharedPreferences.Editor editor = sharedPref.edit();
        editor.clear();
        editor.apply();
    }

    public void deleteDataByKey(Message message, Context context){

        SharedPreferences sharedPref = context.getSharedPreferences(Constants.PREFERENCE_FILE, Context.MODE_PRIVATE);

        HashSet<String> keysToDelete = new HashSet<String>();

        Map<String, ?> keys = sharedPref.getAll();

        for (Map.Entry<String, ?> entry : keys.entrySet()) {

            if(entry.getKey().split(Constants.KEY_VERSION_SEPARATOR)[0].equals(message.getKey())){
                keysToDelete.add(entry.getKey());

            }

        }

        SharedPreferences.Editor editor = sharedPref.edit();

        Iterator<String> itr = keysToDelete.iterator();

        while(itr.hasNext()){
            editor.remove(itr.next());

        }

        editor.apply();

    }

    public HashMap<String, String> convertPacketsToKeyPair(String packets) {

        HashMap<String, String> hm = new HashMap<String, String>();

        List<String> ls = Arrays.asList(packets.split(Constants.LIST_SEPARATOR));

        for (String packet : ls) {
            if (packet != null && packet.trim().length() > 0) {
                Message msg = new Message(packet);

                //TODO: put the latest version only, hint: sort increasing, so last value overwrites

                hm.put(msg.getKey().split(Constants.KEY_VERSION_SEPARATOR)[0], msg.getValue());

            }
        }

        return hm;
    }

    public String getAllLocalData(Context context) {

        HashMap<String, String> hm = new HashMap<String, String>();

        SharedPreferences sharedPref = context.getSharedPreferences(Constants.PREFERENCE_FILE, Context.MODE_PRIVATE);

        Map<String, ?> keys = sharedPref.getAll();

        for (Map.Entry<String, ?> entry : keys.entrySet()) {

            hm.put(entry.getKey(), entry.getValue().toString());

        }

        return convertMessageListToPacket(hm);

    }


    public String getDataByKey(Message message, Context context) {

        HashMap<String, String> hm = new HashMap<String, String>();

        SharedPreferences sharedPref = context.getSharedPreferences(Constants.PREFERENCE_FILE, Context.MODE_PRIVATE);

        Map<String, ?> keys = sharedPref.getAll();

        for (Map.Entry<String, ?> entry : keys.entrySet()) {

            if(entry.getKey().split(Constants.KEY_VERSION_SEPARATOR)[0].equals(message.getKey())){
                hm.put(entry.getKey(), entry.getValue().toString());

            }

        }

        return convertMessageListToPacket(hm);

    }

    public void returnPacketAsAcknoldegement(Socket clientSocket, String packets) throws IOException{


        DataOutputStream dataOutputStream = new DataOutputStream(clientSocket.getOutputStream());
        dataOutputStream.writeUTF(packets);
        dataOutputStream.flush();

        dataOutputStream.close();

    }


    public String convertMessageListToPacket(HashMap<String, String> hm) {


        List<String> packetList = new ArrayList<String>();


        Iterator it = hm.entrySet().iterator();

        while (it.hasNext()) {
            Map.Entry<String, String> pair = (Map.Entry) it.next();

            Message msg = new Message();
            msg.setKey(pair.getKey());
            msg.setValue(pair.getValue());
            msg.setMessageType(MessageType.GET);

            packetList.add(msg.createPacket());

            it.remove(); // avoids a ConcurrentModificationException
        }


        return TextUtils.join(Constants.LIST_SEPARATOR, packetList);

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
