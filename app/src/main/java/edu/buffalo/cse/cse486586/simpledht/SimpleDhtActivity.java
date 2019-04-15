package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.net.Uri;

import android.os.AsyncTask;
import android.os.Bundle;
import android.app.Activity;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.widget.TextView;

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
import java.util.TreeMap;

public class SimpleDhtActivity extends Activity {

    static final String TAG = SimpleDhtActivity.class.getSimpleName();
    static String[] REMOTE_PORT = new String[]{"11108", "11112", "11116", "11120", "11124"};
    TreeMap<String,Node> ring = new TreeMap<String, Node>();
    static String myId="";
    static final int SERVER_PORT = 10000;
    private ContentResolver mContentResolver;
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";
    private int key = -1;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_simple_dht_main);

        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        switch(Integer.parseInt(myPort)){
            case(11108): myId = "5554"; break;
            case(11112): myId = "5556"; break;
            case(11116): myId = "5558"; break;
            case(11120): myId = "5560"; break;
            case(11124): myId = "5562"; break;
        }

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            return;
        }


        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());
        findViewById(R.id.button3).setOnClickListener(
                new OnTestClickListener(tv, getContentResolver()));
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }



    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_simple_dht_main, menu);
        return true;
    }


/*http://developer.android.com/reference/android/os/AsyncTask.html*/
private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

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

                if(ReqType.equalsIgnoreCase("REQ_JOIN")){
                    performJoinOperation(rect[0]);
                }else if(recStr.equalsIgnoreCase("REQ_KEY")){
                    storeKey();
                }

                input.close();
                sc.close();
            }catch (SocketTimeoutException e) {
                Log.e(TAG, "ServerTask SocketTimeoutException");
            } catch (IOException e) {
                Log.e(TAG, "ServerTask IOException");
            }catch (Exception e){
                Log.e(TAG, "ServerTask Other Exception");
            }
        }
    }

    private void storeKey() {
    }

    private String performJoinOperation(String key) {
        String hash="";
        try {
            hash = genHash(key);
            if(ring.size() ==0){
                Node newNode = new Node(key,hash);
                newNode.setSucc(newNode);
                newNode.setPred(newNode);
                ring.put(hash,newNode);
            }else{
                Node lowerNode = ring.lowerEntry(hash).getValue();
                Node higherNode = ring.higherEntry(hash).getValue();
                Node newNode = new Node(key,hash);
                if(lowerNode !=null && higherNode != null){
                    newNode.setPred(lowerNode);
                    newNode.setSucc(higherNode);

                    lowerNode.setSucc(newNode);
                    higherNode.setPred(newNode);
                }else if(lowerNode ==null){
                    Node lowestNode = ring.firstEntry().getValue();
                    Node highestNode = ring.lastEntry().getValue();
                    newNode.setPred(highestNode);
                    newNode.setSucc(lowestNode);

                    lowestNode.setPred(newNode);
                    highestNode.setSucc(newNode);
                }
            }

            for(Map.Entry<String,Node> node : ring.entrySet()){
                Log.v("RING_SETUP",node.getValue().hash+"~"+node.getValue().key+"~"+node.getValue().pred+"~"+node.getValue().succ);
            }
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return hash;
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

    protected void onProgressUpdate(String...strings) {

        String strReceived = strings[0].trim();

        ContentValues cv = new ContentValues();
        cv.put(KEY_FIELD, Integer.toString(key));
        cv.put(VALUE_FIELD, strReceived);

        Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.groupmessenger2.provider");
        mContentResolver = getContentResolver();
        mContentResolver.insert(mUri, cv);
    }
}


private class ClientTask extends AsyncTask<String, Void, Void>{

    @Override
    protected Void doInBackground(String... msgs) {
        String msg = msgs[0];
        DataInputStream ackRec;
        DataOutputStream output;
        Socket socket;

        try{
            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(REMOTE_PORT[0]));

            String msgToSend = "REQ_JOIN"+"~"+myId;
            //Log.v("Client Request:",msgToSend+" Port:"+remotePort);
            output = new DataOutputStream(socket.getOutputStream());
            output.writeUTF(msgToSend);

            ackRec = new DataInputStream(socket.getInputStream());
            String ackStr = ackRec.readUTF();
            //Log.v("Client Ack from Server:",ackStr+" Port:"+remotePort);

            if(ackRec.equals("Ack")){
                output.close();
                ackRec.close();
                socket.close();
            }
        }catch (SocketTimeoutException e) {
            Log.e(TAG, "ClientTask SocketTimeoutException1");
        } catch (UnknownHostException  e) {
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
        Node pred;
        Node succ;

        Node(String key, String hash){
            this.key = key;
            this.hash = hash;
            this.pred = null;
            this.succ = null;
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
