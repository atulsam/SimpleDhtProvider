package edu.buffalo.cse.cse486586.simpledht;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider {
    FeedReaderDbHelper dbHelper;
    String TABLE_NAME = "pa2b";
    String COLUMN_NAME_TITLE = "key";
    String COLUMN_NAME_SUBTITLE = "value";
    static final String TAG = SimpleDhtProvider.class.getSimpleName();

    static TreeMap<String,Node> ring = new TreeMap<String, Node>();
    static String myId="";
    static final int SERVER_PORT = 10000;
    static String[] REMOTE_PORT = new String[]{"11108", "11112", "11116", "11120", "11124"};
    static final String KEY_FIELD = "key";
    static final String VALUE_FIELD = "value";
    Node myInfo;
    private HashMap<String,String> keyValueMap = new HashMap<String, String>();
    private HashMap<String,String> portIdMap = new HashMap<String, String>();
    private String myPort =  "";
    private String requestedPort = "";
    ClientTask clientTask = new ClientTask();
    private StringBuilder receivedKeyValueQuery = new StringBuilder();
    private StringBuilder myKeyVal = new StringBuilder();
    boolean signal1=false;
    boolean signal2=false;
    @Override
    public boolean onCreate() {
        dbHelper = new FeedReaderDbHelper(getContext());
        SQLiteDatabase db = dbHelper.getWritableDatabase();
        db.delete(TABLE_NAME, null, null);

        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        requestedPort = myPort;
        switch(Integer.parseInt(myPort)){
            case(11108): myId = "5554"; break;
            case(11112): myId = "5556"; break;
            case(11116): myId = "5558"; break;
            case(11120): myId = "5560"; break;
            case(11124): myId = "5562"; break;
        }

        portIdMap.put("5554","11108");
        portIdMap.put("5556","11112");
        portIdMap.put("5558","11116");
        portIdMap.put("5560","11120");
        portIdMap.put("5562","11124");

        clientTask.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"REQ_JOIN");

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            e.printStackTrace();
        }

        return false;
    }


    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        /*https://developer.android.com/training/data-storage/sqlit*/

        SQLiteDatabase  db = dbHelper.getWritableDatabase();;
        String[] selections = selection.split(":");
        String requestedPort= "";
        String whereClause = COLUMN_NAME_TITLE + " = ? ";
        String[] whereArgs = new String[]{selections[0]};
        signal1 = false;
        if(selections.length>1){
            requestedPort = selections[1];
        }
//        int deletedRows =0;
        if(myInfo == null || (myInfo.pred.hash.equalsIgnoreCase(myInfo.succ.hash) && myInfo.hash.equalsIgnoreCase(myInfo.succ.hash))){
            if(selection.equalsIgnoreCase("*")){
                db.delete(TABLE_NAME, null, null);
            }else  if(selection.equalsIgnoreCase("@")){
                db.delete(TABLE_NAME, null, null);
            }else{
                db.delete(TABLE_NAME, whereClause, whereArgs);
            }
        }else{
            if(selection.equalsIgnoreCase("*")){

                if(requestedPort.equals("")){
                    selection = selection+":"+myPort;
                    Log.v("DELETE_ALL Start Node",selection);
                    clientTask.doInBackground("DELETE_ALL",selection);

                    while (!signal2){}

                    db.delete(TABLE_NAME, null, null);
                    signal1 = true;
                }else if(requestedPort.equals(myPort)){
                    Log.v("DELETE_ALL End Node",selection);
                    signal1 = true;
                    return 0;
                }else{
                    Log.v("DELETE_ALL Other Node",selection);
                    clientTask.doInBackground("DELETE_ALL",selection);
                    while (!signal2){}
                    db.delete(TABLE_NAME, null, null);
                    signal1 = true;
                }

            }else if(selection.equalsIgnoreCase("@")){
                db.delete(TABLE_NAME, null, null);
                Log.v("DELETE@",myId);
                signal1 = true;
            }

            else {
                    int deletedRow = db.delete(TABLE_NAME, whereClause, whereArgs);
                    if(requestedPort.equals("")){
                        selection = selection+":"+myPort;
                    }

                    if(deletedRow <=0){
                        Log.v("DELETE@",selection+"~"+requestedPort);
                        if(requestedPort.equals(myPort)){
                            signal1 = true;
                            return 0;
                        }else{
                            clientTask.doInBackground("DELETE_KEY",selection);
                            while (!signal2){}
                        }
                    }
                    signal1 = true;
                }

            }

//        Log.v("Deleted Rows: ",deletedRows+"");
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        try{
            String actualKey = (String) values.get(COLUMN_NAME_TITLE);
            String value = (String) values.get(COLUMN_NAME_SUBTITLE);
            String key = genHash(actualKey);
            Log.v("KEY_HASH",key+" ActualKey: "+actualKey);
            if(myInfo == null || (myInfo.pred.hash.equalsIgnoreCase(myInfo.succ.hash) && myInfo.hash.equalsIgnoreCase(myInfo.succ.hash))){
                handleInsert(uri,values);
            }else{
                if(key.compareTo(myInfo.hash) < 0 && key.compareTo(myInfo.pred.hash) > 0){
                    handleInsert(uri,values);
                }else if(!myPort.equals("11108")){
                    clientTask.doInBackground("FWD_KEY",actualKey,value);
                    Log.v("Sent Key : ", key);
                }else{
                    Entry e1 = ring.lowerEntry(key);
                    Entry e2 = ring.higherEntry(key);

                    if(e2 != null && e1 != null){
                        Node n = (Node) e2.getValue();
                        if(n.port.equalsIgnoreCase(myPort)){
                            handleInsert(uri,values);
                        }else{
                            AsyncTask.Status status = clientTask.getStatus();
                            Log.v("STATUS: ", status.toString());
                            clientTask.doInBackground("SEND_KEY",actualKey,value,n.port);
                        }
                        Log.v("Sent2 Key : ", key+" port:"+n.port);
                    }else{
                        Entry eLow = ring.firstEntry();
                        Node n = (Node) eLow.getValue();
                        if(n.port.equalsIgnoreCase(myPort)){
                            handleInsert(uri,values);
                        }else{
                            clientTask.doInBackground("SEND_KEY",actualKey,value,n.port);
                        }
                        Log.v("Sent1 Key : ", key+" port:"+n.port);
                    }
                }
            }
        }catch(Exception e){
            e.printStackTrace();
            //Log.v("Exception",""+e);
        }
        return uri;
    }

    public long handleInsert(Uri uri, ContentValues values){
        long newRowId;
        String key = (String) values.get(COLUMN_NAME_TITLE);
        SQLiteDatabase db = dbHelper.getWritableDatabase();
        newRowId = db.replaceOrThrow(TABLE_NAME, null, values);
        Log.v("Inserted Key: ", key);
        return newRowId;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs, String sortOrder) {
        Cursor cursor = null;
        myKeyVal = new StringBuilder();
        signal1 = false;
        // https://stackoverflow.com/questions/10600670/sqlitedatabase-query-method
        String[] selections = selection.split(":");
        String requestedPort= "";
        String whereClause = COLUMN_NAME_TITLE + " = ? ";
        String[] whereArgs = new String[]{selections[0]};

        if(selections.length>1){
            requestedPort = selections[1];
        }

        try{
            SQLiteDatabase db = dbHelper.getReadableDatabase();

            if(myInfo == null || (myInfo.pred.hash.equalsIgnoreCase(myInfo.succ.hash) && myInfo.hash.equalsIgnoreCase(myInfo.succ.hash))){
                Log.v("Single Node",selection);
                if(selection.equalsIgnoreCase("*")){
                    cursor = db.query(TABLE_NAME, null, null, null,null,null,null);
                }else if(selection.equalsIgnoreCase("@")){
                    cursor = db.query(TABLE_NAME, null, null, null,null,null,null);
                }else{
                    cursor = db.query(TABLE_NAME,null, whereClause, whereArgs, null, null, null);
                }
            }else{
                if(whereArgs[0].equalsIgnoreCase("*")){
                    Log.v("QUERY_ALL *",selection+"~"+receivedKeyValueQuery);
                    if(requestedPort.equals("")){
                        receivedKeyValueQuery = new StringBuilder();
                        selection = selection+":"+myPort;
                        Log.v("QUERY_ALL Start Node",selection+"~"+receivedKeyValueQuery);
                        clientTask.doInBackground("QUERY_ALL",selection);
                        while (!signal2){}

                        cursor = db.query(TABLE_NAME, null, null, null,null,null,null);
                        MatrixCursor c1 = getKeyValuePairCursor();

                        Cursor c2 = mergeCursor(cursor,c1);
                        generateReceivedKeyVal(c2);
                        signal1 = true;
                        return c2;
                    }else if(requestedPort.equals(myPort)){
                        Log.v("QUERY_ALL End Node",selection+"~"+receivedKeyValueQuery);
                        signal1 = true;
                        return null;
                    }else{
                        receivedKeyValueQuery = new StringBuilder();
                        Log.v("QUERY_ALL Other Node",selection+"~"+receivedKeyValueQuery);
                        clientTask.doInBackground("QUERY_ALL",selection);
                        while (!signal2){}

                        cursor = db.query(TABLE_NAME, null, null, null,null,null,null);
                        MatrixCursor c1 = getKeyValuePairCursor();
                        Log.v("Other Cursor",cursor.getCount()+"");
                        Cursor c2 = mergeCursor(cursor,c1);
                        generateReceivedKeyVal(c2);
                        signal1 = true;
                        return c2;
                    }

                }else if(selection.equalsIgnoreCase("@")){
                    cursor = db.query(TABLE_NAME, null, null, null,null,null,null);
                    Log.v("QUERY@@",cursor.getCount()+"~"+myId);
                    return cursor;
                }

                else{

                    cursor = db.query(TABLE_NAME,null, whereClause, whereArgs, null, null, null);

                    if(requestedPort.equals("")){
                        if (cursor.getCount() == 0){
                            selection = selection+":"+myPort;
                            Log.v("QUERY_FWD1@KEY",selection+"~"+myId);
                            clientTask.doInBackground("FWD_QUERY",selection);
                            while(receivedKeyValueQuery.length() == 0){}
                            cursor = getKeyValuePairCursor();
                            generateKeyVal(cursor);
                            return cursor;

                        }else{
                            generateKeyVal(cursor);
                            return cursor;
                        }
                    }else{
                        if (cursor.getCount() == 0){
                            Log.v("QUERY_FWD2@KEY",selection+"~"+myId);
                            clientTask.doInBackground("FWD_QUERY",selection);
                            while(receivedKeyValueQuery.length() == 0){}
                            cursor = getKeyValuePairCursor();
                            generateKeyVal(cursor);
                            return cursor;
                        }else{
                            generateKeyVal(cursor);
                            return cursor;
                        }
                    }

                }
            }


        }catch(Exception e){
            e.printStackTrace();
            Log.v("Exception", e+"");
            signal2 = true;
        }
        return cursor;
    }

    private MatrixCursor getKeyValuePairCursor(){

        String[] colNames = {"key","value"};
        MatrixCursor cursor1 = new MatrixCursor(colNames);

        Log.v("getKeyValuePairCursor1", receivedKeyValueQuery+"~"+myId);
        if(receivedKeyValueQuery.length() >0){
            String[] keyValList = receivedKeyValueQuery.toString().split("#");
            for(String str : keyValList){
                String[] keyVal = str.split("@");
                cursor1.addRow(keyVal);
            }
        }
        Log.v("getKeyValuePairCursor2", cursor1.getCount()+"");
        return cursor1;
    }

    private MatrixCursor mergeCursor(Cursor cur , MatrixCursor c1){

        Log.v("mergeCursor", cur.getCount()+"~"+c1.getCount());

        while(cur.moveToNext()){
            String[] keyVal = {cur.getString(0),cur.getString(1)};
            c1.addRow(keyVal);
        }

        return c1;
    }

    private void generateReceivedKeyVal(Cursor cursor){
        while (cursor.moveToNext()){
            receivedKeyValueQuery.append(cursor.getString(0)+"@"+cursor.getString(1)+"#");
            Log.v("Updating KeyValL",receivedKeyValueQuery.toString());
        }
    }

    private void generateKeyVal(Cursor cursor){
        while (cursor.moveToNext()){
            myKeyVal.append(cursor.getString(0)+"@"+cursor.getString(1)+"#");
            Log.v("Updating KeyValL",myKeyVal.toString());
        }
    }
    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        String str = "key = '"+selection+"'";
        SQLiteDatabase db = dbHelper.getWritableDatabase();
        int count = db.update(TABLE_NAME, values, str, selectionArgs);
//        Log.v("update Row: ", count+"");
        return 0;
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


    /*https://developer.android.com/training/data-storage/sqlit*/

    public class FeedReaderDbHelper extends SQLiteOpenHelper {
        // If you change the database schema, you must increment the database version.
        public static final int DATABASE_VERSION = 1;
        public static final String DATABASE_NAME = "FeedReader";
        private final String SQL_CREATE_ENTRIES ="CREATE TABLE IF NOT EXISTS " + TABLE_NAME + " ( " +
                COLUMN_NAME_TITLE+" TEXT," +
                COLUMN_NAME_SUBTITLE + " TEXT)";
        //private final String SQL_DELETE_ENTRIES ="DROP TABLE IF EXISTS " + TABLE_NAME;

        public FeedReaderDbHelper(Context context) {
            super(context, DATABASE_NAME, null, DATABASE_VERSION);

        }
        @Override
        public void onCreate(SQLiteDatabase db) {
            db.execSQL(SQL_CREATE_ENTRIES);
        }
        public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
            //db.execSQL(SQL_DELETE_ENTRIES);
            onCreate(db);
        }
        public void onDowngrade(SQLiteDatabase db, int oldVersion, int newVersion) {
            onUpgrade(db, oldVersion, newVersion);
        }
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    /*http://developer.android.com/reference/android/os/AsyncTask.html*/
    class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            while(true){
                try{
                    Socket sc = serverSocket.accept();
                    /*https://stackoverflow.com/questions/28187038/tcp-client-server-program-datainputstream-dataoutputstream-issue*/
                    DataInputStream input = new DataInputStream(sc.getInputStream());
                    String recStr = input.readUTF();
                    String[] rect = recStr.split("~");
                    String ReqType = rect[0];

//                    Log.v("****Server ReQ****",recStr);
                    if(ReqType.equalsIgnoreCase("REQ_JOIN")){
                        String avdId = rect[1];
                        performJoinOperation(avdId);

                        String replyToClient = "JOINED_ACK";
                        DataOutputStream output = new DataOutputStream(sc.getOutputStream());
                        output.writeUTF(replyToClient);

                        for(Entry<String,Node> nodeEntry : ring.entrySet()) {
                            Node nq = nodeEntry.getValue();
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(nq.port));

                            String brdMsg = "NODE_INFO"+"~"+nq.key+"~"+nq.hash+"~"+nq.pred.key+"~"+nq.pred.hash
                                    +"~"+nq.succ.key+"~"+nq.succ.hash;

                            DataOutputStream broadStream = new DataOutputStream(socket.getOutputStream());
                            broadStream.writeUTF(brdMsg);
                        }
                    }else if(ReqType.equalsIgnoreCase("NODE_INFO")){
                        myInfo = new Node(rect[1],rect[2],rect[3],rect[4],rect[5],rect[6]);
//                        Log.v("MYINFO",myInfo.key+myInfo.pred.key+myInfo.succ.key);
                    }else if(ReqType.equalsIgnoreCase("FWD_KEY")){
                        publishProgress("FWD_KEY",rect[1],rect[2]);
//                        Log.v("SER_FWD_KEY",rect[1]+"~"+genHash(rect[1]));

                        String replyToClient = "FWD_KEY_ACK";
                        DataOutputStream output = new DataOutputStream(sc.getOutputStream());
                        output.writeUTF(replyToClient);

                    }else if(ReqType.equalsIgnoreCase("SEND_KEY")){
                        publishProgress("SEND_KEY",rect[1],rect[2]);
//                        Log.v("SER_SEND_KEY",rect[1]+"~"+genHash(rect[1]));

                        String replyToClient = "SEND_KEY_ACK";
                        DataOutputStream output = new DataOutputStream(sc.getOutputStream());
                        output.writeUTF(replyToClient);
                    }else if(ReqType.equalsIgnoreCase("FWD_QUERY")){
                        myKeyVal = new StringBuilder();
                        publishProgress(rect[0],rect[1]);
                        Log.v("SER_FWD_KEY",rect[1]);

                        while(myKeyVal.length() == 0){}

                        String replyToClient = "QUERY_ACK"+"~"+myKeyVal;
                        DataOutputStream output = new DataOutputStream(sc.getOutputStream());
                        output.writeUTF(replyToClient);
                        Log.v("SER_FWD_KEY1",replyToClient);
                    }else if(ReqType.equalsIgnoreCase("QUERY_ALL")){
                        signal1 = false;
                        signal2 = false;
                        publishProgress(rect[0],rect[1]);
                        Log.v("SER_QUERY_ALL",rect[1]);

                        while(!signal1){}

                        String replyToClient = "QUERY_ALL_ACK"+"~"+receivedKeyValueQuery;
                        DataOutputStream output = new DataOutputStream(sc.getOutputStream());
                        output.writeUTF(replyToClient);
                        Log.v("SER_FWD_KEY1",replyToClient);
                    }else if(ReqType.equalsIgnoreCase("DELETE_ALL") || ReqType.equalsIgnoreCase("DELETE_KEY")){
                        signal1 = false;
                        signal2 = false;
                        publishProgress(rect[0],rect[1]);
                        Log.v("SER_DELETE",rect[1]);
                        while(!signal1){}
                        String replyToClient = "DELETE_ACK";
                        DataOutputStream output = new DataOutputStream(sc.getOutputStream());
                        output.writeUTF(replyToClient);
                        Log.v("SER_DELETE",replyToClient);
                    }

//                    input.close();
//                    sc.close();
                }catch (SocketTimeoutException e) {
                    Log.e(TAG, "ServerTask SocketTimeoutException");
                } catch (IOException e) {
                    Log.e(TAG, "ServerTask IOException");
                }catch (Exception e){
                    Log.e(TAG, "ServerTask Other Exception ");
                    e.printStackTrace();
                }
            }
        }

        @Override
        protected void onProgressUpdate(String...strings) {

            Log.v("PublishProgress",strings[0]+"~"+strings[1]);
            Othertask other = new Othertask();
            other.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,strings);
        }

        private String printRing() {
            String test="";
            for(Map.Entry<String, Node> node : ring.entrySet()){
//                test += node.getValue().key+" "+node.getValue().pred.getKey()+" "+node.getValue().succ.key+"^^";
                test += node.getValue().key+" "+node.getValue().succ.key+"^^";
            }
            return test;
        }

        private Node performJoinOperation(String key) {
            Node newNode = null;
            try {
                String port = portIdMap.get(key);
                String hash = genHash(key);
                newNode = new Node(key,hash);
                newNode.setPort(port);
                Log.v("KEY",key+" "+hash);

                if(ring.size() ==0){
                    newNode.setSucc(newNode);
                    newNode.setPred(newNode);
                    ring.put(hash,newNode);
                }if(ring.size() == 1){
                    Node fn = ring.firstEntry().getValue();
                    newNode.setSucc(fn);
                    newNode.setPred(fn);
                    fn.setPred(newNode);
                    fn.setSucc(newNode);
                    ring.put(hash,newNode);
                }else{
                    String lowerNodeEntry = ring.lowerKey(hash);
                    String higherNodeEntry = ring.higherKey(hash);

                    if(lowerNodeEntry !=null && higherNodeEntry != null){
                        Node lowerNode = ring.lowerEntry(hash).getValue();
                        Node higherNode = ring.higherEntry(hash).getValue();

                        newNode.setPred(lowerNode);
                        newNode.setSucc(higherNode);

                        lowerNode.setSucc(newNode);
                        higherNode.setPred(newNode);
                    }else if(lowerNodeEntry !=null ){
                        Node lowestNode = ring.firstEntry().getValue();
                        Node highestNode = ring.lastEntry().getValue();

                        lowestNode.setPred(newNode);
                        highestNode.setSucc(newNode);

                        newNode.setPred(highestNode);
                        newNode.setSucc(lowestNode);
                    }else if(higherNodeEntry != null){
                        Node lowestNode = ring.firstEntry().getValue();
                        Node highestNode = ring.lastEntry().getValue();

                        lowestNode.setPred(newNode);
                        highestNode.setSucc(newNode);

                        newNode.setPred(highestNode);
                        newNode.setSucc(lowestNode);
                    }
                    ring.put(hash,newNode);
                }
                Log.v("PRINT",printRing());
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }catch (Exception e){
                e.printStackTrace();
            }
            return newNode;
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


    }

    class Othertask extends AsyncTask<String, Void, Void>{

        @Override
        protected Void doInBackground(String... strings) {
            String param = strings[0];
            if(param.equalsIgnoreCase("SEND_KEY")){
                String key = strings[1];
                String value = strings[2];
                ContentValues cv = new ContentValues();
                cv.put(KEY_FIELD, key);
                cv.put(VALUE_FIELD, value);
                Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
                handleInsert(mUri, cv);
            }else if(param.equalsIgnoreCase("FWD_KEY")){
                String key = strings[1];
                String value = strings[2];
                ContentValues cv = new ContentValues();
                cv.put(KEY_FIELD, key);
                cv.put(VALUE_FIELD, value);
                Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
                insert(mUri,cv);
            }else if(param.equalsIgnoreCase("SEND_QUERY") || param.equalsIgnoreCase("FWD_QUERY") || param.equalsIgnoreCase("QUERY_ALL")){
                String key = strings[1];
                Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
                Log.v("OtherTask",key);
                query(mUri,null,key,null,null);
            }else if(param.equalsIgnoreCase("DELETE_ALL") || param.equalsIgnoreCase("DELETE_KEY")){
                String key = strings[1];
                Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
                delete(mUri,key,null);
            }
            return null;
        }
    }


    class ClientTask extends AsyncTask<String, Void, Void>{

        @Override
        protected Void doInBackground(String... msgs) {
            DataInputStream ackRec;
            DataOutputStream output;
            Socket socket;
            String msg = msgs[0];
//            Log.v("Client Task",msgs[0]);
            try{
                if(msg.equalsIgnoreCase("REQ_JOIN")){
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(REMOTE_PORT[0]));
                    String msgToSend = msg+"~"+myId;
//                    Log.v("Client Request:",msgToSend+" Port:"+myPort+" MyId: "+myId);
                    output = new DataOutputStream(socket.getOutputStream());
                    output.writeUTF(msgToSend);

                    ackRec = new DataInputStream(socket.getInputStream());
                    String ackStr = ackRec.readUTF();
//                    Log.v("Client Ack from Server:",ackStr+" Port:"+REMOTE_PORT[0]);
                    String[] receivedInfo = ackStr.split("~");

                    if(receivedInfo[0].equals("JOINED_ACK")){
//                        Log.v("Client Connection","Closed");
                        output.close();
                        ackRec.close();
                        socket.close();
                    }
                }else if(msg.equalsIgnoreCase("FWD_KEY")){
                    String port = portIdMap.get(myInfo.succ.key);
                    String msgToSend = msg+"~"+msgs[1]+"~"+msgs[2];
//                    Log.v("FWD_KEY:",msgToSend+" myPort:"+myPort+" port: "+port);

                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(port));
                    output = new DataOutputStream(socket.getOutputStream());
                    output.writeUTF(msgToSend);

                    ackRec = new DataInputStream(socket.getInputStream());
                    String ackStr = ackRec.readUTF();
                    String[] receivedInfo = ackStr.split("~");

                    if(receivedInfo[0].equals("FWD_KEY_ACK")){
                        output.close();
                        ackRec.close();
                        socket.close();
                    }

                }else if(msg.equalsIgnoreCase("SEND_KEY")){
                    String port = msgs[3];
                    String msgToSend = msg+"~"+msgs[1]+"~"+msgs[2];
//                    Log.v("SEND_KEY:",msgToSend+" myPort:"+myPort+" port: "+port);

                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(port));
                    output = new DataOutputStream(socket.getOutputStream());
                    output.writeUTF(msgToSend);

                    ackRec = new DataInputStream(socket.getInputStream());
                    String ackStr = ackRec.readUTF();
                    String[] receivedInfo = ackStr.split("~");

                    if(receivedInfo[0].equals("SEND_KEY_ACK")){
                        output.close();
                        ackRec.close();
                        socket.close();
                    }
                }
                else if(msg.equalsIgnoreCase("FWD_QUERY")){
                    receivedKeyValueQuery = new StringBuilder();
                    String port = portIdMap.get(myInfo.succ.key);
                    String msgToSend = msg+"~"+msgs[1];
                    Log.v("FWD_QUERY:",msgToSend+" from myPort:"+myPort+" port: "+port);

                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(port));
                    output = new DataOutputStream(socket.getOutputStream());
                    output.writeUTF(msgToSend);

                    ackRec = new DataInputStream(socket.getInputStream());
                    String ackStr = ackRec.readUTF();
                    String[] receivedInfo = ackStr.split("~");

                    if(receivedInfo[0].equals("QUERY_ACK")){
//                        Log.v("Client Connection","Closed");
                        receivedKeyValueQuery.append(receivedInfo[1]);
                        output.close();
                        ackRec.close();
                        socket.close();
                    }
                }else if(msg.equalsIgnoreCase("QUERY_ALL")){
                    receivedKeyValueQuery = new StringBuilder();
                    String port = portIdMap.get(myInfo.succ.key);
                    String msgToSend = msg+"~"+msgs[1];
                    Log.v("QUERY_ALL:",msgToSend+" from myPort:"+myPort+" port: "+port);

                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(port));
                    output = new DataOutputStream(socket.getOutputStream());
                    output.writeUTF(msgToSend);

                    ackRec = new DataInputStream(socket.getInputStream());
                    String ackStr = ackRec.readUTF();
                    String[] receivedInfo = ackStr.split("~");

                    if(receivedInfo[0].equals("QUERY_ALL_ACK")){
                        if(receivedInfo.length >1)
                            receivedKeyValueQuery.append(receivedInfo[1]);
                        signal2 = true;
                        output.close();
                        ackRec.close();
                        socket.close();
                    }
                }else if(msg.equalsIgnoreCase("DELETE_ALL") || msg.equalsIgnoreCase("DELETE_KEY")){
                    String port = portIdMap.get(myInfo.succ.key);
                    String msgToSend = msg+"~"+msgs[1];
                    Log.v("DELETE:",msgToSend+" from myPort:"+myPort+" port: "+port);

                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(port));
                    output = new DataOutputStream(socket.getOutputStream());
                    output.writeUTF(msgToSend);

                    ackRec = new DataInputStream(socket.getInputStream());
                    String ackStr = ackRec.readUTF();
                    String[] receivedInfo = ackStr.split("~");

                    if(receivedInfo[0].equals("DELETE_ACK")){
                        signal2 = true;
                        output.close();
                        ackRec.close();
                        socket.close();
                    }
                }

            }catch (SocketTimeoutException e) {
                Log.e(TAG, "ClientTask SocketTimeoutException1");
            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask socket UnknownHostException1");
            }catch(IOException e){
                Log.e(TAG, "ClientTask socket IOException1");
            }finally{

            }
            return null;
        }

    }

    class Node{
        String key;
        String hash;
        String port;
        String predHash;
        String SuccHash;
        Node pred;
        Node succ;

        Node(String key, String hash){
            this.key = key;
            this.hash = hash;
            this.pred = null;
            this.succ = null;
        }

        Node(String key, String hash, String predKey, String predHash, String succKey, String succHash){
            this.key = key;
            this.hash = hash;
            this.pred = new Node(predKey,predHash);
            this.succ = new Node(succKey,succHash);
        }

        public String getPort() {
            return port;
        }

        public void setPort(String port) {
            this.port = port;
        }



        public String getPredHash() {
            return predHash;
        }

        public void setPredHash(String predHash) {
            this.predHash = predHash;
        }

        public String getSuccHash() {
            return SuccHash;
        }

        public void setSuccHash(String succHash) {
            SuccHash = succHash;
        }


        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public String getHash() {
            return hash;
        }

        public void setHash(String hash) {
            this.hash = hash;
        }

        public Node getPred() {
            return pred;
        }

        public void setPred(Node pred) {
            this.pred = pred;
        }

        public Node getSucc() {
            return succ;
        }

        public void setSucc(Node succ) {
            this.succ = succ;
        }
    }
}

