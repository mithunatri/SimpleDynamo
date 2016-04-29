package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;

import java.util.Collections;
import java.util.Formatter;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Semaphore;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.database.Cursor;
import android.content.Context;
import android.database.MatrixCursor;
import android.database.MergeCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

    static final String TAG = SimpleDynamoProvider.class.getSimpleName();
    static final int SERVER_PORT = 10000;

    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";
    private Map<String, Integer> msgType;
    private static ArrayList<String> node_hash_ring;
    private static ArrayList<String> node_ring;

    int REPLICATION_FACTOR = 3;
    String[] columnNames = {KEY_FIELD,VALUE_FIELD};
    String node_Id;
    String portStr;

    String clientResponse;
    String waitingOnKey = null;
    Semaphore sema = new Semaphore(0, true);
    Semaphore query_sema = new Semaphore(0,true);

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
        Log.d(TAG,"Delete: " + selection);

        int numberOfRows = 0;
        String storageNode = portStr;
        String[] fileList = null;

        String[] parts = selection.split(";");
        String key = parts[0];

        if(key.equals("*")){
            String totalRows = gdumpDel(uri);

            return  Integer.parseInt(totalRows);
        }
        else if(key.equals("@")){
            fileList = getContext().fileList();
        }   
        else{
            if(parts.length == 1)
                storageNode = determineStorageNode(key, "");

            if(storageNode.equals(portStr)) {
                fileList = new String[]{key};
                int replicaDelCount = deleteReplicas(key, REPLICATION_FACTOR);

                Log.d(TAG, "Replica Delete Count: " + replicaDelCount);
            }
            else {
                return transferDelete(key, storageNode);
            }
        }

        for(String file:fileList){
            try {
                if (getContext().deleteFile(file))
                    numberOfRows++;
                else
                    throw new RuntimeException("Delete of file " + file + " failed");
            }catch (RuntimeException e){
                Log.e(TAG,e.getLocalizedMessage());
            }
        }

        return numberOfRows;
	}

    /**
     * Key does not belong to this node, so transfer to the node responsible for the
     * key.
     * @param key
     * @param storageNode
     * @return total rows deleted on remote AVD
     */
    private int transferDelete(String key, String storageNode){
        String nodeToQuery = convertToPort(storageNode);
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                                                nodeToQuery, "DELETE", key);

        sema_acquire(sema);

        if(clientResponse.isEmpty())  return -1;

        int deleteReplicaRows = Integer.parseInt(clientResponse);
        //Release response sema?
        return deleteReplicaRows;
    }

    private int deleteReplicas(String key, int replicaCount){

        int myNodeIndex = node_ring.indexOf(portStr);
        int replicaNodeIndex =  (myNodeIndex + 1) % node_ring.size();
        String replicaNodePort = convertToPort(node_ring.get(replicaNodeIndex));
        clientResponse = "";
        --replicaCount;
        String content = key + "," + String.valueOf(replicaCount);

        if(replicaCount > 0) {
            Log.d(TAG, "Replication Count: " + replicaCount);
            new ClientTask2().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, replicaNodePort,
                    "RDELETE", content);

            //sema_acquire(sema);
        }
        if (clientResponse.isEmpty()) return 0;

        int deleteReplicaRows = Integer.parseInt(clientResponse);
        //Release response sema?
        return deleteReplicaRows;
    }

    private int replicateDelete(String key, int replicateCount){

        int numberOfRows = 0;

        try {
            if (getContext().deleteFile(key))
                numberOfRows++;
            else
                throw new RuntimeException("Delete of file " + key + " failed");
        }catch (RuntimeException e){
            Log.d(TAG,e.getLocalizedMessage());
        }

        Log.d(TAG, "Replica delete at AVD " + portStr + "  :" + numberOfRows);

        int deleteReplicaRows  = deleteReplicas(key, replicateCount);

        return deleteReplicaRows + numberOfRows;
    }

    /**
     * Performs the delete operation for "*".
     * @param uri
     * @param originPort
     * @return number of rows affected
     */
    private String gdumpDel(Uri uri){

        Log.d(TAG,"Global Dump Delete starting with AVD: " + portStr);
        int gDumpResponse = 0;
        int selfRowsAffected = getContext().getContentResolver().delete(uri,"@",null);

        for (String node: node_ring){
            if(node.equals(portStr)) continue;
            String nodeToQuery = convertToPort(node);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                                                        nodeToQuery, "DELETE", "@");

            sema_acquire(sema);
            Log.d(TAG, "gDumpDel response from AVD " + node + " :" + clientResponse);
            if(!clientResponse.isEmpty())
                gDumpResponse += Integer.valueOf(clientResponse);
        }

        if(gDumpResponse == 0)
            return String.valueOf(selfRowsAffected);

        int totalRowsAffected = selfRowsAffected + gDumpResponse;

        return String.valueOf(totalRowsAffected);
    }

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
        String combo = (String) values.get(KEY_FIELD);
        String value = (String) values.get(VALUE_FIELD);

        /**
         * If Insert is being invoked from ServerTask, it means that it has been forwarded by another
         * AVD. Hence, key will now be a combo of "key;MSGTYPE". As the AVD which forwarded the
         * request has already determined that this AVD is the storage node, no need to determine
         * the storage node again.
         */
        String[] parts = combo.split(";");
        String key = parts[0];
        String storageNode = portStr;
        Log.d(TAG,"Insert for key: " + key);
        if(parts.length == 1)
            storageNode = determineStorageNode(key,value);

        Log.d(TAG, "Storage node for Key-value pair is: " + storageNode);
        if(storageNode.equals(portStr)) {
            insertToFs(key, value);
            //Replicate
            replicate(key, value, REPLICATION_FACTOR);

        }else{
            //Create new client task and send message.
            Log.d(TAG," Ready to forward the request");
            String message = key + "," + value;
            String node = convertToPort(storageNode);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                                                        node, "INSERT", message);
        }

        return uri;
    }

    /**
     * Method which sends key, value pair to current node's replica.
     * @param key
     * @param value
     */
    public void replicate(String key, String value, int replicaCount){
        int nodeRing_length = node_ring.size();

        int myNodeIndex = node_ring.indexOf(portStr);
        int replicaIndex = (myNodeIndex + 1) % nodeRing_length;
        String replicaPort = convertToPort(node_ring.get(replicaIndex));

        Log.d(TAG, "Replicate Key-Value pair to AVD: " + Integer.parseInt(replicaPort) / 2);

        if (--replicaCount > 0) {
            Log.d(TAG,"Replica count is: " + replicaCount);
            String message = key + "," + value + "," + String.valueOf(replicaCount);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                                            replicaPort, "REPLICATE", message);

            //sema_acquire();
        }
    }

    public void replicateInsert(String key, String value, int replicateCount){
        insertToFs(key, value);
        Log.d(TAG, "Replica insert done on AVD: " + portStr);

        Log.d(TAG, "Send replica to other nodes");
        replicate(key, value, replicateCount);

        //return "Done";
    }

    public void insertToFs(String key, String value){
        FileOutputStream file;

        try {
            file = getContext().openFileOutput(key, Context.MODE_PRIVATE);
            file.write(value.getBytes());
            file.close();
        } catch (Exception e) {
            Log.e("insert", e.getMessage());
            e.printStackTrace();
        }
        if(key.equals(waitingOnKey)){
            waitingOnKey = null;
            query_sema.release();
        }
        Log.v("insert", key + "," + value);
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
        Log.d(TAG,"Query method: " + selection);

        String storageNode = portStr;
        String[] fileList = null;

        String[] parts = selection.split(";");
        String key = parts[0];

        if(key.equals("*")){
            return gdump(uri);
        }
        else if(key.equals("@")){
            fileList = getContext().fileList();
        }
        else{
            if(parts.length == 1) {
                storageNode = determineStorageNode(key, "");
                Log.d(TAG, "Storage node for Key-value pair is: " + storageNode);
                String readNode = determineReadNode(storageNode);
                Log.d(TAG,"Query read node: " + readNode);
                if(!readNode.equals(portStr))
                    return transferQuery(key, readNode);
            }

            if(!checkKeyExists(key)){
                Log.d(TAG,"Key does not exist. I will wait till insertion is done");
                blockTillInsert(key);
            }

            fileList = new String[]{key};
        }

        try {
            MatrixCursor cursor = new MatrixCursor(columnNames);

            for(String files : fileList) {
                Log.d(TAG,"Searching for file: " + files);
                BufferedReader in = new BufferedReader(
                        new InputStreamReader(getContext().openFileInput(files)));
                if (in == null)
                    throw new Exception();

                String value = in.readLine();
                Log.d(TAG,"Value for key: " + files + " Value: " + value);
                in.close();
                MatrixCursor.RowBuilder builder = cursor.newRow();
                builder.add(KEY_FIELD, files);
                builder.add(VALUE_FIELD, value);
            }
            Log.d(TAG,"Query Done.");
            return cursor;
        } catch (Exception e) {
            Log.v("query", e.getMessage());
        }
        Log.d(TAG,"Don't come here!");
        return null;
	}

    private boolean checkKeyExists(String key){
        File directory = getContext().getFilesDir();
        File file = new File(directory.toString() + "/" + key);
        Log.d(TAG,"File: " + file.toString());
        return file.exists();
    }

    private void blockTillInsert(String key){
        waitingOnKey = key;
        sema_acquire(query_sema);
    }

    /**
     * Transfer query to appropriate node and return Cursor.
     * @param selection
     * @param nodeToQuery
     * @return
     */
    private Cursor transferQuery(String selection, String nodeToQuery){

        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                                                convertToPort(nodeToQuery), "QUERY", selection);

        sema_acquire(sema);

        if(clientResponse.isEmpty())  return null;

        Cursor cursor = createCursor(clientResponse);
        return cursor;
    }

    private String determineReadNode(String storageNode){
        int storageNodeIndex = node_ring.indexOf(storageNode);
        int nodeToQuery_Index = (storageNodeIndex + 2) % node_ring.size();
        //String nodeToQuery = convertToPort(node_ring.get(nodeToQuery_Index));
        String nodeToQuery = node_ring.get(nodeToQuery_Index);

        if(nodeToQuery.equals(portStr))
            return portStr;

        return nodeToQuery;
    }

    /**
     * Query operation for "*"
     * @param uri
     * @return
     */
    private Cursor gdump(Uri uri){
        Log.d(TAG,"Global Dump starting at AVD: "  + portStr);
        String gDumpResponse = "";
        Cursor cursor = getContext().getContentResolver().query(uri, null, "@", null, null);

        for (String node: node_ring){
            if(node.equals(portStr)) continue;
            String nodeToQuery = convertToPort(node);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                                                        nodeToQuery, "QUERY", "@");

            sema_acquire(sema);
            Log.d(TAG, "gDump response from AVD " + node + " :" + clientResponse);
            if(!clientResponse.isEmpty())
                gDumpResponse += clientResponse + ";";
            //Release response Sema?
        }

        if(gDumpResponse == null || gDumpResponse.isEmpty())
            return cursor;
        Log.d(TAG,"Global Dump: " + gDumpResponse);
        gDumpResponse = gDumpResponse.replaceAll(";$","");
        Cursor mCursor = createCursor(gDumpResponse);

        MergeCursor combinedCursor = new MergeCursor(new Cursor[]{cursor,mCursor});

        return combinedCursor;
    }

    /**
     * Creates a cursor by iterating over a String response with results separated by ;
     * @param response
     * @return
     */
    private Cursor createCursor(String response){
        Log.d(TAG,"Creating cursor for:"  + response);
        String[] keyValuePairs = response.split(";");

        MatrixCursor mCursor = new MatrixCursor(columnNames);

        for(String keyValue : keyValuePairs){
            String[] parts = keyValue.split(",");

            MatrixCursor.RowBuilder builder = mCursor.newRow();
            builder.add(KEY_FIELD, parts[0]);
            builder.add(VALUE_FIELD, parts[1]);
        }
        //Release response semaphore?
        return mCursor;
    }


    @Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

    public String determineStorageNode(String key,String value){

        try{

            String hashKey = genHash(key);
            String sendToNode = null;

            int nodeRing_length = node_ring.size();

            for(int i=0; i < nodeRing_length; i++){
                int prevNodeIndex = i;
                int succNodeIndex = (prevNodeIndex + 1) % nodeRing_length;
                Log.d(TAG, "Determine Storage Node:  " + node_ring.get(i));
                if(succNodeIndex == 0 &&
                        hashKey.compareTo(node_hash_ring.get(prevNodeIndex)) > 0) {
                    Log.d(TAG,"Last node condition");
                    sendToNode = node_ring.get(succNodeIndex);
                    break;
                }
                else if (hashKey.compareTo(node_hash_ring.get(prevNodeIndex)) > 0 &&
                                hashKey.compareTo(node_hash_ring.get(succNodeIndex)) <= 0){
                    sendToNode = node_ring.get(succNodeIndex);
                    break;
                }
                else if(prevNodeIndex == 1 &&
                        hashKey.compareTo(node_hash_ring.get(prevNodeIndex)) < 0) {
                    sendToNode = node_ring.get(0);
                    break;
                }
            }

            return sendToNode;

        }catch(NoSuchAlgorithmException e){
            Log.e(TAG, e.getLocalizedMessage());
            e.printStackTrace();
        }

        return null;
    }

	@Override
	public boolean onCreate() {

		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(
																	Context.TELEPHONY_SERVICE);
        portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr)) * 2);
        Log.d(TAG,"AVD Number:" + portStr);
        try{
            node_Id = genHash(portStr);
            createMembershipRing();
        }catch(NoSuchAlgorithmException e){
            e.printStackTrace();
        }

        Log.d(TAG, "Starting server task...");
        try{
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);

            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

        }catch (IOException e){
            Log.e(TAG, "Error while creating socket:" + e.getLocalizedMessage());
        }

    	return false;
	}

    /**
     * Creates membership ring.
     * @throws NoSuchAlgorithmException
     */
    public void createMembershipRing() throws NoSuchAlgorithmException{
        TreeMap<String, String> nodeId_map = new TreeMap<String,String>();
        node_hash_ring = new ArrayList<String>();
        String[] nodes = new String[]{"5554","5556","5558","5560","5562"};

        for (String node : nodes) {
            String hash = genHash(node);
            nodeId_map.put(hash, node);
            node_hash_ring.add(hash);
        }

        Collections.sort(node_hash_ring);
        node_ring = new ArrayList<String>(nodeId_map.values());

        Log.d(TAG,"Membership ring: " + node_ring.toString());
        Log.d(TAG, "Membership Hash Ring: " + node_hash_ring);
    }

	private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private String convertToPort(String avd_number){
        return String.valueOf(Integer.parseInt(avd_number) * 2);
    }

    private void sema_acquire(Semaphore sema){
        Log.d(TAG,"Acquiring semaphore. May block. Semaphore: " + sema.toString());
        try{
            sema.acquire();
        }catch(InterruptedException e){
            e.printStackTrace();
        }
        Log.d(TAG,"Acquired semaphore." + sema.toString());
    }

    //#--------------------------------------------------------------------------------------------#
    public class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {

            Log.d(TAG, "Server Task beginning...");
            msgType = new HashMap<String, Integer>();
            assignMsgTypes();

            ServerSocket serverSocket = sockets[0];
            try {
                while (true) {

                    try {
                        Socket socket = serverSocket.accept();

                        createNewThread(socket);

                    } catch (IOException e) {
                        e.printStackTrace();
                        Log.e(TAG, "IOException. Reason: " + e.getLocalizedMessage());
                    }

                }
            } catch (Exception e) {
                e.printStackTrace();
                Log.e(TAG, "General Exception. Reason: " + e.getLocalizedMessage());
            }


            return null;
        }

        private void createNewThread(Socket socket){
            new Task().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, socket);
            //task(socket);
        }

        private void assignMsgTypes() {

            //Only AVD0 will receive request with JOIN.
            msgType.put("QUERY", 1);
            msgType.put("INSERT", 2);
            msgType.put("REPLICATE", 3);
            msgType.put("DELETE", 4);
            msgType.put("RDELETE", 5);

        }
    }



    //#--------------------------------------------------------------------------------------------#
    public class Task extends AsyncTask<Socket, Void, Void>{

        public Task(){
            Log.d(TAG,"Task Created..");
        }

        @Override
        protected Void doInBackground(Socket... sockets){
        //private void task(Socket socket){
            try {
                Socket socket = sockets[0];
                DataInputStream reader = new DataInputStream(socket.getInputStream());
                String incomingMsg = reader.readUTF();

                Log.d(TAG, "Received Message: " + incomingMsg);
                String[] parts = incomingMsg.split("\\|");

                int type = msgType.get(parts[0]);
                String response = null;
                switch (type) {
                    case 1:
                        response = queryRequest(parts[1]);
                        break;
                    case 2:
                        insertRequest(parts[1]);
                        break;
                    case 3:
                        replicateRequest(parts[1]);
                        break;
                    case 4:
                        response = deleteRequest(parts[1]);
                        break;
                    case 5:
                        response = deleteReplicaRequest(parts[1]);
                        break;
                    default:
                        throw new IOException("Incorrect message type: "
                                + type);

                }

                if(response != null && !response.isEmpty()) {
                    Log.d(TAG, "Responding from server.");
                    DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                    out.writeUTF(response);
                }
                socket.close();
            }catch(IOException e){
                e.printStackTrace();
                Log.e(TAG, "IOException. Reason: " + e.getLocalizedMessage());
            }
            Log.d(TAG,"Task Exiting...");
            return null;
        }

        private String queryRequest(String incomingReq){

            String[] parts = incomingReq.split(",");

            String key = parts[0].trim() + ";QUERY";
            Log.d(TAG,"Query request received from another AVD:" + parts[0]);
            ContentProviderHelper cp = new ContentProviderHelper();
            Uri uri = cp.buildUri();

            Cursor cursor = getContext().getContentResolver().query(uri,null,key,null,null);

            if (cursor == null)
                return null;

            int keyIndex = cursor.getColumnIndex("key");
            int valueIndex = cursor.getColumnIndex("value");
            String response = "";
            while(cursor.moveToNext()){
                String resultKey = cursor.getString(keyIndex);
                String resultValue = cursor.getString(valueIndex);
                String result = resultKey + "," + resultValue;

                response +=  result + ";";
            }

            cursor.close();

            response = response.replaceAll(";$", "");

            return response;
        }

        /**
         * Handle incoming Insert request from other AVD.
         * @param incomingReq
         * @return
         */
        private void insertRequest(String incomingReq) {
            ;
            String[] parts = incomingReq.split(",");
            String key = parts[0].trim() + ";INSERT" ;
            String value = parts[1].trim();
            Log.d(TAG,"Insert request received from another avd:" + parts[0]);

            ContentProviderHelper cp = new ContentProviderHelper();
            Uri uri = cp.buildUri();

            ContentValues cv = cp.buildContentValues(key, value);
            getContext().getContentResolver().insert(uri, cv);
            Log.d(TAG,"Insertion done");
        }

        /**
         * Handle incoming replication request.
         * @param incomingReq
         */
        private void replicateRequest(String incomingReq){

            String[] parts = incomingReq.split(",");
            String key = parts[0].trim();
            String value = parts[1].trim();
            int replicaCount = Integer.parseInt(parts[2]);
            Log.d(TAG, "Replicate request received from another AVD:" + parts[0]);
            replicateInsert(key, value, replicaCount);
            Log.d(TAG,"Replication done");


        }

        private String deleteRequest(String incomingReq){


            String[] parts = incomingReq.split(",");
            String key = parts[0].trim() + ";DELETE";
            Log.d(TAG,"Delete existing replica request received from another AVD:" + parts[0]);
            ContentProviderHelper cp = new ContentProviderHelper();
            Uri uri = cp.buildUri();

            int numberOfRows = getContext().getContentResolver().delete(uri, key, null);

            return String.valueOf(numberOfRows);
        }

        private String deleteReplicaRequest(String incomingReq) {
            Log.d(TAG, "Delete replica request received from another AVD");
            String[] parts = incomingReq.split(",");
            String key = parts[0].trim();
            int replicaCount = Integer.parseInt(parts[1].trim());

            int numberOfRows = replicateDelete(key, replicaCount);

            return String.valueOf(numberOfRows);
        }
    }



    //#--------------------------------------------------------------------------------------------#
    public class ClientTask extends AsyncTask<String, Void, Void>{

        public ClientTask(){
            Log.d(TAG,"Client Task Created");
        }

        @Override
        protected Void doInBackground(String... message) {

            Log.d(TAG,"Within Client send message..");
            String node = message[0];
            String msgType = message[1].trim();
            String finalMessage = msgType + "|" + message[2];

            Log.d(TAG,"Final Message to be sent: " + finalMessage + " to Port:" + node);
            Log.d(TAG, "Message Type: " + msgType);
            Socket socket = null;
            clientResponse = "";
            try {
                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(node));

                DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                DataInputStream in = new DataInputStream(socket.getInputStream());

                out.writeUTF(finalMessage);

                if(!msgType.equals("INSERT") && !msgType.equals("REPLICATE")) {
                    Log.d(TAG,"Insert and Replicate should not enter this block.");
                    String response = in.readUTF();
                    Log.d(TAG, "Response received from Port: " + node + " Response: " + response);
                    clientResponse = response;
                    sema.release();
                }
            }catch(IOException e){
                Log.e(TAG, "IOException while delivering message. Reason: " +
                                                                        e.getLocalizedMessage());
                e.printStackTrace();
            }finally{
                try{
                    socket.close();
                    Log.d(TAG,"Client task closing..");
                }catch(Exception e){
                    e.printStackTrace();
                }
            }

            return null;
        }
    }

    public class ClientTask2 extends AsyncTask<String, Void, Void>{

        public ClientTask2(){
            Log.d(TAG,"Client Task2 Created");
        }

        @Override
        protected Void doInBackground(String... message) {

            Log.d(TAG,"Within Client send message..");
            String node = message[0];
            String msgType = message[1].trim();
            String finalMessage = msgType + "|" + message[2];

            Log.d(TAG,"Final Message to be sent: " + finalMessage + " to Port:" + node);
            Log.d(TAG, "Message Type: " + msgType);
            Socket socket = null;
            clientResponse = "";
            try {
                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(node));

                DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                DataInputStream in = new DataInputStream(socket.getInputStream());

                out.writeUTF(finalMessage);

                if(!msgType.equals("INSERT") && !msgType.equals("REPLICATE")) {
                    Log.d(TAG,"Insert and Replicate should not enter this block.");
                    String response = in.readUTF();
                    Log.d(TAG, "Response received from Port: " + node + " Response: " + response);
                    clientResponse = response;
                    sema.release();
                }
            }catch(IOException e){
                Log.e(TAG, "IOException while delivering message. Reason: " +
                        e.getLocalizedMessage());
                e.printStackTrace();
            }finally{
                try{
                    socket.close();
                    Log.d(TAG,"Client task2 closing..");
                }catch(Exception e){
                    e.printStackTrace();
                }
            }

            return null;
        }
    }
   /*public String send(String node, String msgType, String content){

       Log.d(TAG,"Within Client send message..");
       String finalMessage = msgType + "|" + content;
       Log.d(TAG,"Final Message to be sent: " + finalMessage);
       String response = null;

       try {
           Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                   Integer.parseInt(node));

           DataOutputStream out = new DataOutputStream(socket.getOutputStream());
           DataInputStream in = new DataInputStream(socket.getInputStream());

           out.writeUTF(finalMessage);

           String response = in.readUTF();
           Log.d(TAG,"Response received from Port: " + node + " Response: " + response);
           if(!msgType.equals("INSERT") || !msgType.equals("REPLICATE"))
                    clientResponse = response;

       }catch(IOException e){
           Log.e(TAG, "IOException while delivering message. Reason: " + e.getLocalizedMessage());
           e.printStackTrace();
       }

       return response;
   }
*/
}