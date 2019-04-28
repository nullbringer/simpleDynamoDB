package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.DataInputStream;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {


    private static final String TAG = SimpleDynamoProvider.class.getName();

    private static String MY_PORT;
    private static String MACHINE_ID;
	private static TreeMap<String, String> ringStructure = new TreeMap<String, String>();

	private ProviderHelper providerHelper;

    @Override
    public boolean onCreate() {

        providerHelper = new ProviderHelper();


        // get my port

        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        MACHINE_ID = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        MY_PORT = String.valueOf(Integer.parseInt(MACHINE_ID) * 2);

        try {


            providerHelper.deleteAllLocalData(getContext());



            //initialize ringStructure
            ringStructure.put(providerHelper.genHash("5554"),"5554");
            ringStructure.put(providerHelper.genHash("5556"),"5556");
            ringStructure.put(providerHelper.genHash("5558"),"5558");
            ringStructure.put(providerHelper.genHash("5560"),"5560");
            ringStructure.put(providerHelper.genHash("5562"),"5562");



            /* Create server */

            ServerSocket serverSocket = new ServerSocket(Constants.SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);





        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG,"Could not create Ring structure");
        } catch(IOException e){
            Log.e(TAG,"Could not create Server!");
        }


        /* get data from siblings and replicate */

        replicateSiblings();


        return true;
    }

    private class ServerTask extends AsyncTask<ServerSocket, Message, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];

            /* infinite while loop to accept multiple messeges */

            while (true) {

                try {

                    Socket clientSocket = serverSocket.accept();

                    DataInputStream dataInputStream = new DataInputStream(clientSocket.getInputStream());
                    String incomingMessege = dataInputStream.readUTF();


                    Message msg = new Message(incomingMessege);


                    Log.d(TAG, msg.toString());

                    switch (msg.getMessageType()) {


                        case STORE:
                            providerHelper.saveKeyPairInDataStore(msg, getContext());
                            providerHelper.returnStandardAcknoldegement(clientSocket);
                            break;

                        case GET:
                            providerHelper.returnPacketAsAcknoldegement(clientSocket,
                                    providerHelper.getDataByKey(msg, getContext()));
                            break;

                        case GET_ALL:
                            providerHelper.returnPacketAsAcknoldegement(clientSocket,
                                    providerHelper.getAllLocalData(getContext()));
                            break;

                        case DEL:
                            providerHelper.deleteDataByKey(msg, getContext());
                            providerHelper.returnStandardAcknoldegement(clientSocket);
                            break;

                        case DEL_ALL:
                            providerHelper.deleteAllLocalData(getContext());
                            providerHelper.returnStandardAcknoldegement(clientSocket);
                            break;

                        default:

                            Log.e(TAG,"blank messege recieved!!");

                    }

                    clientSocket.close();

                } catch (IOException e) {
                    Log.e(TAG, "Client Connection failed");
                }
            }

        }

    }

    private class ClientTask extends AsyncTask<String, Void, String> {

        @Override
        protected String doInBackground(String... msgs) {

            String msg = msgs[0];
            int thisPort = Integer.parseInt(msgs[1]);
            Log.d(TAG,"targetPort::"+thisPort);

            String result = "";

            /*
             * Send messeges to target node
             * */

            try {

                Socket socket = providerHelper.connectAndWriteMessege(thisPort, msg);
                result = providerHelper.readAckAndClose(socket);

                socket.close();

            } catch (SocketTimeoutException e) {
                Log.e(TAG, "ClientTask SocketTimeoutException");
                result = Constants.FAILED_NODE_INDICATOR;

            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
                result = Constants.FAILED_NODE_INDICATOR;

            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException: ");
                result = Constants.FAILED_NODE_INDICATOR;

            }

            return result;
        }
    }




	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {

        Message message = new Message();
        message.setMessageType(MessageType.DEL);
        message.setKey(selection);

        if(selection.equals(Constants.GLOBAL_INDICATOR)){

            /* If *, delete all data from all the nodes */

            message.setMessageType(MessageType.DEL_ALL);


            for(Map.Entry<String,String> entry : ringStructure.entrySet()) {

                String value = entry.getValue();

                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message.createPacket(),
                        String.valueOf(Integer.parseInt(value)*2));
            }



        } else if(selection.equals(Constants.LOCAL_INDICATOR)){

            /* If @, delete all data from local node */

            providerHelper.deleteAllLocalData(getContext());

        } else{

            /* If others, delete data by key from network */

            try {

                LinkedHashSet<String> targetNodes = providerHelper.getTargetNodesForKey(message, ringStructure);

                Iterator<String> itr = targetNodes.iterator();

                while(itr.hasNext()){

                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message.createPacket(),
                            String.valueOf(Integer.parseInt(itr.next())*2));

                }

            } catch (NoSuchAlgorithmException e) {
                Log.e(TAG, "Could not hash!!");
            }

        }



		return 0;
	}

	@Override
	public String getType(Uri uri) {

		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {

        String thisKey = values.getAsString(Constants.KEY_FIELD);
        String thisValue = values.getAsString(Constants.VALUE_FIELD);


        Message message = new Message();
        message.setMessageType(MessageType.STORE);
        message.setOrigin(MY_PORT);
        message.setKey(thisKey);
        message.setValue(thisValue);
        message.setOriginTimestamp(String.valueOf(System.currentTimeMillis()));


        saveInRing(message);

        return uri;
	}


	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {


        //Log.v("query", selection);

        LinkedHashMap<String, String> hm = new LinkedHashMap<String, String>();

        Message message = new Message();
        message.setOrigin(String.valueOf(MY_PORT));
        message.setMessageType(MessageType.GET);
        message.setKey(selection);

        if (selection.equals(Constants.GLOBAL_INDICATOR)) {

            //Get all data from all the nodes

            message.setMessageType(MessageType.GET_ALL);

            StringBuilder result= new StringBuilder();

            for(Map.Entry<String,String> entry : ringStructure.entrySet()) {

                String value = entry.getValue();

                try {

                    String returnedDataset = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message.createPacket(),
                           String.valueOf(Integer.parseInt(value)*2)).get();

                    if(!returnedDataset.equals(Constants.FAILED_NODE_INDICATOR)){
                        result.append(Constants.LIST_SEPARATOR).append(returnedDataset);

                    }



                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            }


            // Get only the latest keypairs
            hm = providerHelper.convertPacketsToKeyPair(result.toString());


        } else if (selection.equals(Constants.LOCAL_INDICATOR)) {

            //Get all data from local node

            String returnedDataset = providerHelper.getAllLocalData(getContext());

            // Get only the latest keypairs
            hm = providerHelper.convertPacketsToKeyPair(returnedDataset);

        } else {

            // Get data by key from ring

            try {

                LinkedHashSet<String> targetNodes = providerHelper.getTargetNodesForKey(message, ringStructure);

                Log.d(TAG,"Target for " + selection +":::"+targetNodes.toString());

                Iterator<String> itr = targetNodes.iterator();

                while(itr.hasNext()){

                    try {

                        String target = itr.next();

                        String returnedDataset = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message.createPacket(),
                                String.valueOf(Integer.parseInt(target)*2)).get();



                        if(returnedDataset.length()==0){

                            Log.e(TAG,"Returned from " +target+ " for " + selection +":::"+returnedDataset);

//                            throw new RuntimeException("This is a crash");



                        }

                        if(!returnedDataset.equals(Constants.FAILED_NODE_INDICATOR)  && returnedDataset.length()>0){
                            hm = providerHelper.convertPacketsToKeyPair(returnedDataset);

                            // get data from all nodes, so don't use break
                            //break;

                        }


                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (ExecutionException e) {
                        e.printStackTrace();
                    }

                }

            } catch (NoSuchAlgorithmException e) {
                Log.e(TAG, "Could not hash!!");
            }




        }

        MatrixCursor cursor = new MatrixCursor(
                new String[]{Constants.KEY_FIELD, Constants.VALUE_FIELD}
        );

        for (Map.Entry<String, String> entry : hm.entrySet()) {

            cursor.newRow()
                    .add(Constants.KEY_FIELD, entry.getKey())
                    .add(Constants.VALUE_FIELD, entry.getValue());
        }

        return cursor;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {

		return 0;
	}

    private void saveInRing(Message message) {

        try {

            LinkedHashSet<String> targetNodes = providerHelper.getTargetNodesForKey(message, ringStructure);

            Iterator<String> itr = targetNodes.iterator();

            while(itr.hasNext()){

                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message.createPacket(),
                        String.valueOf(Integer.parseInt(itr.next())*2));

            }

        } catch (NoSuchAlgorithmException e) {

            Log.e(TAG, "Could not hash!!");

        }


    }

	private void replicateSiblings(){

        try {

            LinkedHashSet<String> siblingNodes = providerHelper.getSiblingNodes(MY_PORT, ringStructure);


            Message message = new Message();
            message.setOrigin(String.valueOf(MY_PORT));
            message.setMessageType(MessageType.GET_ALL);

            Iterator<String> itr = siblingNodes.iterator();

            StringBuilder result= new StringBuilder();

            while(itr.hasNext()){

                try {

                    String returnedDataset = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message.createPacket(),
                            String.valueOf(Integer.parseInt(itr.next())*2)).get();

                    if(!returnedDataset.equals(Constants.FAILED_NODE_INDICATOR)){
                        result.append(Constants.LIST_SEPARATOR).append(returnedDataset);

                    }

                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }

            }



            List<Message>  msgList = providerHelper.convertPacketToMessageList(result.toString());

            List<Message> myMsgList = new ArrayList<Message>();

            for(Message msg: msgList){

                LinkedHashSet<String> targetNodes = providerHelper.getTargetNodesForKey(msg, ringStructure);

                if(targetNodes.contains(MACHINE_ID)){

                    myMsgList.add(msg);

                }


            }


            providerHelper.saveKeyPairListInDataStore(myMsgList, getContext());



        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }


    }

}
