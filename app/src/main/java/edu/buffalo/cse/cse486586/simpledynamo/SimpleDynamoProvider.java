package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.TreeMap;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {


    private static final String TAG = SimpleDynamoProvider.class.getName();

    private static String MY_PORT;
	private static TreeMap<String, String> ringStructure = new TreeMap<String, String>();

	private ProviderHelper providerHelper = new ProviderHelper(getContext());

    @Override
    public boolean onCreate() {


        // get my port

        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        MY_PORT = String.valueOf(Integer.parseInt(portStr) * 2);

        try {
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
                            providerHelper.saveKeyPairInDataStore(msg);
                            providerHelper.returnStandardAcknoldegement(clientSocket);
                            break;

                        case GET:
                            String packets = "";

                            if (!msg.getOrigin().equals(MY_PORT)) packets = getGlobalData(msg);

                            /* Send back the retrieved through channel to caller (Previous node) */

                            DataOutputStream dataOutputStream = new DataOutputStream(clientSocket.getOutputStream());
                            dataOutputStream.writeUTF(packets);
                            dataOutputStream.flush();

                            dataOutputStream.close();

                            break;

                        case DEL:
                            //if (!msg.getOrigin().equals(MY_PORT)) deleteFromRing(msg);

                            break;

                        default:

                            Log.e(TAG,"blank messege recieved!!");

                    }

                    clientSocket.close();

                } catch (IOException e) {
                    Log.e(TAG, "Client Connection failed");
                } catch (NoSuchAlgorithmException e) {
                    Log.e(TAG, "Could not hash!!");
                }
            }

        }

    }

    private class ClientTask extends AsyncTask<String, Void, String> {

        @Override
        protected String doInBackground(String... msgs) {

            String msg = msgs[0];
            int thisPort = Integer.parseInt(msgs[1]);

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

            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");

            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException: ");

            }

            return result;
        }
    }

    private void saveInRing(Message message) {

        try {

            LinkedHashSet<String> targetNodes = providerHelper.getTargetNodesForKey(message, ringStructure);

            Iterator<String> itr = targetNodes.iterator();

            while(itr.hasNext()){

                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message.createPacket(), itr.next());

            }

        } catch (NoSuchAlgorithmException e) {

            Log.e(TAG, "Could not hash!!");

        }


    }





	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
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


        saveInRing(message);

        return uri;
	}


	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

}
