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
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class ProviderHelper {


    private String dbMode = Constants.DB_MODE_FREE;

    private int writerWaiting = 0;




    public void saveKeyPairInDataStore(Message message, Context context){


        if (isReadyToWrite()) {
            SharedPreferences sharedPref = context.getSharedPreferences(Constants.PREFERENCE_FILE, Context.MODE_PRIVATE);
            SharedPreferences.Editor editor = sharedPref.edit();

            editor.putString(message.getKey() + Constants.KEY_VERSION_SEPARATOR + message.getOriginTimestamp(),
                    message.getValue());

            editor.apply();
        }

        freeDataBaseLock();
    }

    public void saveKeyPairListInDataStore(List<Message> myMsgList, Context context) {

        if (isReadyToWrite()) {
            SharedPreferences sharedPref = context.getSharedPreferences(Constants.PREFERENCE_FILE, Context.MODE_PRIVATE);
            SharedPreferences.Editor editor = sharedPref.edit();

            for (Message message:myMsgList){

                editor.putString(message.getKey() + Constants.KEY_VERSION_SEPARATOR + message.getOriginTimestamp(),
                        message.getValue());

            }
            editor.apply();
        }

        freeDataBaseLock();
    }

    public void deleteAllLocalData(Context context){

        if(isReadyToWrite()){

            SharedPreferences sharedPref = context.getSharedPreferences(Constants.PREFERENCE_FILE, Context.MODE_PRIVATE);
            SharedPreferences.Editor editor = sharedPref.edit();
            editor.clear();
            editor.apply();


        }

        freeDataBaseLock();


    }


    public void deleteDataByKey(Message message, Context context){

        if (isReadyToWrite()) {
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

        freeDataBaseLock();

    }

    public String getAllLocalData(Context context) {

        HashMap<String, String> hm = new HashMap<String, String>();

        if (isReadyToRead()) {
            SharedPreferences sharedPref = context.getSharedPreferences(Constants.PREFERENCE_FILE, Context.MODE_PRIVATE);

            Map<String, ?> keys = sharedPref.getAll();

            for (Map.Entry<String, ?> entry : keys.entrySet()) {

                hm.put(entry.getKey(), entry.getValue().toString());

            }
        }

        freeDataBaseLock();

        return convertMessageListToPacket(hm);

    }


    public String getDataByKey(Message message, Context context) {

        HashMap<String, String> hm = new HashMap<String, String>();

        if (isReadyToRead()) {
            SharedPreferences sharedPref = context.getSharedPreferences(Constants.PREFERENCE_FILE, Context.MODE_PRIVATE);

            Map<String, ?> keys = sharedPref.getAll();

            for (Map.Entry<String, ?> entry : keys.entrySet()) {

                if(entry.getKey().split(Constants.KEY_VERSION_SEPARATOR)[0].equals(message.getKey())){
                    hm.put(entry.getKey(), entry.getValue().toString());

                }

            }
        }

        freeDataBaseLock();

        return convertMessageListToPacket(hm);

    }


    private boolean isReadyToRead() {

        synchronized (this) {
            while(true){

                if(!this.dbMode.equals(Constants.DB_MODE_WRITE) && writerWaiting<1){
                    int readerNo = 1;

                    if(this.dbMode.contains(Constants.DB_MODE_READ)){

                        String [] temp = this.dbMode.split(Constants.SEPARATOR);

                        readerNo = Integer.parseInt(temp[1]);
                        readerNo++;


                    }

                    this.dbMode = Constants.DB_MODE_READ + Constants.SEPARATOR + String.valueOf(readerNo);
                    return true;

                }
            }
        }


    }


    private boolean isReadyToWrite() {

        writerWaiting++;

        synchronized (this) {

            while(true){

                if(this.dbMode.equals(Constants.DB_MODE_FREE)){

                    this.dbMode = Constants.DB_MODE_WRITE;
                    this.writerWaiting--;
                    return true;

                }
            }
        }


    }


    private void freeDataBaseLock(){


        if(this.dbMode.contains(Constants.DB_MODE_READ)){

            String [] temp = this.dbMode.split(Constants.SEPARATOR);

            int readerNo = Integer.parseInt(temp[1]);
            readerNo--;


            if(readerNo>0) {
                this.dbMode = Constants.DB_MODE_READ + Constants.SEPARATOR + String.valueOf(readerNo);
                return;
            }


        }

        this.dbMode = Constants.DB_MODE_FREE;
    }

    public LinkedHashMap<String, String> convertPacketsToKeyPair(String packets) {

        LinkedHashMap<String, String> hm = new LinkedHashMap<String, String>();

        List<String> ls = Arrays.asList(packets.split(Constants.LIST_SEPARATOR));




        List<Message> messageList = new ArrayList<Message>();



        for (String packet : ls) {
            if (packet != null && packet.trim().length() > 0) {
                Message msg = new Message(packet);

                messageList.add(msg);

            }
        }

        /* sorting the list so that highest version overwrites the lower values in hashmap */
        Collections.sort(messageList);

        for (Message msg : messageList) {
                hm.put(msg.getKey(), msg.getValue());

        }

        return hm;
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



            msg.setKey(pair.getKey().split(Constants.KEY_VERSION_SEPARATOR)[0]);
            msg.setValue(pair.getValue());
            msg.setOriginTimestamp(pair.getKey().split(Constants.KEY_VERSION_SEPARATOR)[1]);
            msg.setMessageType(MessageType.GET);

            packetList.add(msg.createPacket());

            it.remove(); // avoids a ConcurrentModificationException
        }


        return TextUtils.join(Constants.LIST_SEPARATOR, packetList);

    }

    public List<Message> convertPacketToMessageList(String packets){

        List<Message> msgList = new ArrayList<Message>();

        List<String> ls = Arrays.asList(packets.split(Constants.LIST_SEPARATOR));

        for (String packet : ls) {
            if (packet != null && packet.trim().length() > 0) {
                Message msg = new Message(packet);

                msgList.add(msg);

            }
        }

        return msgList;

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

    private String getPrevNode(String key, TreeMap<String, String> ringStructure) throws NoSuchAlgorithmException{

        String hashedKey = genHash(key);

        String targetHash  = ringStructure.lowerKey(hashedKey);

        if(targetHash == null){
            targetHash = ringStructure.lastKey();
        }

        return ringStructure.get(targetHash);

    }

    public LinkedHashSet<String> getSiblingNodes(String myPort , TreeMap<String, String> ringStructure) throws NoSuchAlgorithmException {

        String machineId = String.valueOf(Integer.parseInt(myPort)/2);


        LinkedHashSet<String> targetNodes = new LinkedHashSet<String>();

        targetNodes.add(getNextNode(machineId,ringStructure));
        targetNodes.add(getPrevNode(machineId,ringStructure));

        return targetNodes;
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
