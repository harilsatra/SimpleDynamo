package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.StreamCorruptedException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import static java.lang.Integer.parseInt;

public class SimpleDynamoProvider extends ContentProvider {
	static final String TAG = SimpleDynamoActivity.class.getSimpleName();
	//Hard-coding the 5 redirection ports.
	static final String REMOTE_PORT0 = "11108";
	static final String REMOTE_PORT1 = "11112";
	static final String REMOTE_PORT2 = "11116";
	static final String REMOTE_PORT3 = "11120";
	static final String REMOTE_PORT4 = "11124";
	static final int SERVER_PORT = 10000;
	private static String myAvdPort;
	//This ArrayList stores the keys stored at the particular port.
	List<String> keys = new ArrayList<String>();
	//This TreeMap stores the hash of the port and the corresponding port number ordered according to the hash.
	TreeMap<String,String> node_list = new TreeMap<String,String>();
	//boolean single_node = false;
	//This HashMap stores the query results when the selection is *
	Map<String,String> query_results = new HashMap<String, String>();
	//This HashMap stores the recovery results when a node recovers after failing.
	Map<String,String> recovery_results = new HashMap<String, String>();

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		if(selection.equals("@")) {
			/*
			CASE: Selection = "@" --> Local Delete All Keys
			Step 1: Iterator to iterate over the keys stored in keys ArrayList.
			Step 2: Delete the file having name as each of the keys that are being iterated.
			Step 3: Delete the key list since all the key value pairs from that particular port have also bee deleted.
			*/
			Iterator<String> itr = keys.iterator();
			while(itr.hasNext()){
				File file = new File(itr.next());
				file.delete();
			}
			keys.clear();
		}
		else if(selection.equals("*")) {
			/*
			CASE: Selection = "*" --> Delete All Keys From All Ports.
			Step 1: Delete all the local key value pairs the same way it is done for @.
			Step 2: After deleting the local key value pairs create a new AsyncTask to communicate local deletion of all key value pairs to all the other ports.
			*/
			Iterator<String> itr = keys.iterator();
			while(itr.hasNext()){
				File file = new File(itr.next());
				file.delete();
			}
			keys.clear();
			Node deleter = new Node();
			deleter.msg = "Delete All";
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, deleter);
			try {
				Thread.sleep(600); //400
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		else{
			/*
			CASE: Selection = "single key" --> Delete that particular key Value pair from wherever it is actually stored.
			Step 1: Create the Hash of the key
			Step 2: Find the port from the Tree Map which has least hash value greater than or equal to the hash of the key to be deleted.
			Step 3: If there is no such port (hash(key) > largest hash of ports) then return the port corresponding to the smallest hash value.
			Step 4: After getting the actual port where the key is stored, obtain the replicas which are the next 2 consecutive nodes in the ring.
			Step 5: Create an AsyncTask and communicate with the actual port and the replica ports to delete the key from their respective Content Providers.
			 */
			String delete_hash = null;
			try {
				delete_hash = genHash(selection);
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
			Node correct = new Node();
			Node replica1 = new Node();
			Node replica2 = new Node();

			String correct_hash =new String();
			String correct_port =new String();
			String replica1_hash =new String();
			String replica1_port =new String();
			String replica2_hash =new String();
			String replica2_port =new String();
			if(delete_hash.compareTo(node_list.lastKey()) > 0){
				correct_hash = node_list.firstKey();
				correct_port = node_list.get(correct_hash);
			}
			else {
				correct_hash = node_list.ceilingKey(delete_hash);
				correct_port = node_list.get(correct_hash);
			}
			replica1_hash = node_list.higherKey(correct_hash);
			if(replica1_hash == null){
				replica1_hash = node_list.firstKey();
				replica1_port = node_list.get(replica1_hash);
			}
			else{
				replica1_port = node_list.get(replica1_hash);
			}
			replica2_hash = node_list.higherKey(replica1_hash);
			if(replica2_hash == null){
				replica2_hash = node_list.firstKey();
				replica2_port = node_list.get(replica2_hash);
			}
			else{
				replica2_port = node_list.get(replica2_hash);
			}

			correct.port = correct_port;
			replica1.port = replica1_port;
			replica2.port = replica2_port;

			correct.del = selection;
			replica1.del = selection;
			replica2.del = selection;

			correct.msg = "Delete";
			replica1.msg = "Delete";
			replica2.msg = "Delete";

			Log.v(TAG,"Delete Request received for " + selection + " on " + myAvdPort + ". Redirecting to " + correct.port + " :R1: " + replica1.port + " :R2: " + replica2.port );

			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, correct);
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, replica1);
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, replica2);
			try {
				Thread.sleep(100); //50
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub
        /*
			Step 1: Create the Hash of the key from the ContentValues
			Step 2: Find the port from the Tree Map which has least hash value greater than or equal to the hash of the key to be deleted.
			Step 3: If there is no such port (hash(key) > largest hash of ports) then return the port corresponding to the smallest hash value.
			Step 4: After getting the actual port where the key is stored, obtain the replicas which are the next 2 consecutive nodes in the ring.
			Step 5: Create an AsyncTask and communicate with the actual port and the replica ports to insert the key in their respective Content Providers.
		*/
		String filename = null;
		filename = values.get("key").toString();
		String hash_file = new String();
		String correct_port = new String();
		String replica1_port = new String();
		String replica2_port = new String();
		String correct_hash = new String();
		String replica1_hash = new String();
		String replica2_hash = new String();
		try {
			hash_file = genHash(filename);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		//get_info.hash = hash_file;
		Node correct = new Node();
		Node replica1 = new Node();
		Node replica2 = new Node();
		if(hash_file.compareTo(node_list.lastKey()) > 0){
			correct_hash = node_list.firstKey();
			correct_port = node_list.get(correct_hash);
		}
		else {
			correct_hash = node_list.ceilingKey(hash_file);
			correct_port = node_list.get(correct_hash);
		}
		replica1_hash = node_list.higherKey(correct_hash);
		if(replica1_hash == null){
			replica1_hash = node_list.firstKey();
			replica1_port = node_list.get(replica1_hash);
		}
		else{
			replica1_port = node_list.get(replica1_hash);
		}
		replica2_hash = node_list.higherKey(replica1_hash);
		if(replica2_hash == null){
			replica2_hash = node_list.firstKey();
			replica2_port = node_list.get(replica2_hash);
		}
		else{
			replica2_port = node_list.get(replica2_hash);
		}
		String string = values.get("value").toString();
		Log.v(TAG,"MSG: "+filename+"--->"+correct_port+"-"+replica1_port+"-"+replica2_port);

		correct.port = correct_port;
		replica1.port = replica1_port;
		replica2.port = replica2_port;

		correct.key = filename;
		replica1.key = filename;
		replica2.key = filename;

		correct.value = string;
		replica1.value = string;
		replica2.value = string;

		correct.msg = "Insert";
		replica1.msg = "Insert";
		replica2.msg = "Insert";
		//Log.v(TAG,"Key "+correct.key+" should be inserted in "+correct.port);

		Log.v(TAG,"Insert Request received for " + filename + " on " + myAvdPort + ". Redirecting to " + correct.port + " :R1: " + replica1.port + " :R2: " + replica2.port );

		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, correct);
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, replica1);
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, replica2);
		try {
			Thread.sleep(100); //50
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return uri;
	}

	@Override
	public boolean onCreate() {
		/*
		Two cases when the onCreate() is called:
		Case 1: When the is first opened
		Case 2: When the application crashes and recovers.
		 */
		// TODO Auto-generated method stub
		//Below is a way to obtain the port number of the AVD.
		TelephonyManager tel =  (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		final String myPort = String.valueOf((parseInt(portStr) * 2));
		myAvdPort = myPort;

        //Created a server socket for communication with other AVDs or we can ports.
		try {
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch (IOException e) {
			Log.e(TAG, "Can't create a ServerSocket");
		}
        /*
        Since each port or AVD knows about every other port or AVD, the below snippet will store the hash as key and the corresponding port number as value in a Tree Map
        11124 --> 11112 --> 11108 --> 11116 --> 11120
         */
		try {
			node_list.put(genHash(String.valueOf(Integer.parseInt(REMOTE_PORT0)/2)),REMOTE_PORT0);
			node_list.put(genHash(String.valueOf(Integer.parseInt(REMOTE_PORT1)/2)),REMOTE_PORT1);
			node_list.put(genHash(String.valueOf(Integer.parseInt(REMOTE_PORT2)/2)),REMOTE_PORT2);
			node_list.put(genHash(String.valueOf(Integer.parseInt(REMOTE_PORT3)/2)),REMOTE_PORT3);
			node_list.put(genHash(String.valueOf(Integer.parseInt(REMOTE_PORT4)/2)),REMOTE_PORT4);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		/*
		The following snippet of code is to handle the case 2 of when the onCreate is called.
		A new AsyncTask is created and the Client Task sends this "Recovering" message to all the other AVDs or ports.
		The case when onCreate is called for the first time it will try to contact other nodes which may have not been set up and the corresponding exception has been handled.
		 */
		Node recovered = new Node();
		recovered.msg="Recovering";
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, recovered);
		try {
			Thread.sleep(600); //100
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		return false;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
						String sortOrder) {
		if(selection.equals("@")){
			/*
			CASE: Selection = "@" --> Return All Local Key-Value Pairs
			Step 1: Iterator to iterate over the keys stored in keys ArrayList.
			Step 2: Open the file having name as each of the keys that are being iterated.
			Step 3: Read the file opened and add the key and value (content of the file which is read) pair in a Matrix Cursor
			Step 4: Return te Matrix Cursor
			*/
			Log.v(TAG,"Local STAR Query");
			Iterator<String> itr = keys.iterator();
			String[] columns = {"key","value"};
			MatrixCursor m = new MatrixCursor(columns);
			while(itr.hasNext()){
				FileInputStream fis = null;
				String sel = itr.next();
				try {
					fis = getContext().openFileInput(sel);
				} catch (FileNotFoundException e) {
					Log.e(TAG, "FILE NOT FOUND");
					//e.printStackTrace();
				}
				StringBuffer fileContent = new StringBuffer("");

				byte[] buffer = new byte[1024];
				int n;
				try {
					while ((n = fis.read(buffer)) != -1)
					{
						fileContent.append(new String(buffer, 0, n));
					}
				} catch (Exception e) {
					Log.e(TAG, "COULD NOT READ THE FILE");
					//e.printStackTrace();
				}
				String[] rows = {sel, String.valueOf(fileContent)};
				m.addRow(rows);
				//Log.v("query", selection);
			}
			return m;
		}
		else if(selection.equals("*")){
			/*
			CASE: Selection = "*" --> Return All Key-Value Pairs From All Ports.
			Step 1: Delete all the local key value pairs the same way it is done for @.
			Step 2: After deleting the local key value pairs create a new AsyncTask to communicate local deletion of all key value pairs to all the other ports.
			*/
			Log.v(TAG,"STAR QUERY");
			Node query_info = new Node();
			query_info.query = selection;
			query_info.msg = "Query All";
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, query_info);
			try {
				Thread.sleep(600); //400
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			//Log.v(TAG,"Value for key "+selection+" is "+query_results.get(selection));
			//The matrix cursor constructor takes the names of the columns as a parameter, which in our case will be 'key' and 'value'
			String[] columns = {"key","value"};
			MatrixCursor m = new MatrixCursor(columns);
			//The addRow function adds a new row to the end with the given column values in the same order as mentioned while calling the constructor.
			//We add the key (parameter 'selection') and the content read from the file above as the key value pair in the matrix cursor.
			for (Map.Entry<String, String> entry : query_results.entrySet()) {
				String key = entry.getKey();
				String value = entry.getValue();
				String[] rows = {key, value};
				m.addRow(rows);
				Log.v(TAG,"Value for key "+key+" is "+value);
			}
			query_results.clear();
			//query_results.remove(selection);
			//Log.v("query", selection);
			Log.v(TAG,""+m.getCount());
			return m;
		}
		else{
			/*
			CASE: Selection = "single key" --> Return that particular key Value pair from wherever it is actually stored.
			Step 1: Create the Hash of the key
			Step 2: Find the port from the Tree Map which has least hash value greater than or equal to the hash of the key to be deleted.
			Step 3: If there is no such port (hash(key) > largest hash of ports) then return the port corresponding to the smallest hash value.
			Step 4: After getting the actual port where the key is stored, obtain the replicas which are the next 2 consecutive nodes in the ring.
			Step 5: Create an AsyncTask and communicate with the actual port and the replica ports to find the key value pair from their respective Content Providers.
			 */
			String query_hash = null;
			try {
				query_hash = genHash(selection);
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
			//query_info.hash = query_hash;
			String destination_port = new String();
			String destination_hash = new String();
			String destination2_port = new String();
			String destination2_hash = new String();
			String destination3_port = new String();
			String destination3_hash = new String();
			Node destination = new Node();
			Node destination2 = new Node();
			Node destination3 = new Node();
			if(query_hash.compareTo(node_list.lastKey()) > 0){
				destination_hash = node_list.firstKey();
				destination_port = node_list.get(destination_hash);
			}
			else {
				destination_hash = node_list.ceilingKey(query_hash);
				destination_port = node_list.get(destination_hash);
			}
			destination2_hash = node_list.higherKey(destination_hash);
			if(destination2_hash == null){
				destination2_hash = node_list.firstKey();
				destination2_port = node_list.get(destination2_hash);
			}
			else{
				destination2_port = node_list.get(destination2_hash);
			}
			destination3_hash = node_list.higherKey(destination2_hash);
			if(destination3_hash == null){
				destination3_hash = node_list.firstKey();
				destination3_port = node_list.get(destination3_hash);
			}
			else{
				destination3_port = node_list.get(destination3_hash);
			}
			destination.port = destination_port;
			destination2.port = destination2_port;
			destination3.port = destination3_port;

			destination.query = selection;
			destination2.query = selection;
			destination3.query = selection;

			destination.msg = "Query";
			destination2.msg = "Query";
			destination3.msg = "Query";

			Log.v(TAG,"Querying " + selection + " on " + myAvdPort + ". Redirecting to " + destination.port + "::" +destination2.port+ "::" +destination3.port);

			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, destination);
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, destination2);
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, destination3);
			try {
				Thread.sleep(500); //100
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			Log.v(TAG,"Value for key "+selection+" is "+query_results.get(selection));
			//The matrix cursor constructor takes the names of the columns as a parameter, which in our case will be 'key' and 'value'
			String[] columns = {"key","value"};
			MatrixCursor m = new MatrixCursor(columns);
			//The addRow function adds a new row to the end with the given column values in the same order as mentioned while calling the constructor.
			//We add the key (parameter 'selection') and the content read from the file above as the key value pair in the matrix cursor.
			String[] rows = {selection, query_results.get(selection)};
			query_results.remove(selection);
			m.addRow(rows);
			//Log.v("query", selection);
			return m;
		}

		// TODO Auto-generated method stub
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}
	//This genHash() function is used to generate the hash of a string and it uses the SHA-1 as the Hashing algorithm.
	private String genHash(String input) throws NoSuchAlgorithmException {
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}

	private class ServerTask extends AsyncTask<ServerSocket, String, Void>{
		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];
			try{
				Socket socket;
				ObjectInputStream objFromClient;
				ObjectOutputStream ServerToClient;
				while(true){
					socket = serverSocket.accept();
					//Read the message over the socket
					objFromClient = new ObjectInputStream(socket.getInputStream());
					ServerToClient = new ObjectOutputStream(socket.getOutputStream());
					Node in_node = null;
					try {
						//Log.v(TAG,"Waiting to get a message");
						in_node = (Node) objFromClient.readObject();
						//Log.v(TAG,"Read the object from client "+in_node.msg);
					} catch (ClassNotFoundException e) {
						Log.e(TAG,"ClassNotFoundException in Server Class");
						e.printStackTrace();
					}
					if(in_node.msg.equals("Insert")){
						keys.add(in_node.key);
						FileOutputStream outputStream;
						try {
							outputStream = getContext().openFileOutput(in_node.key, Context.MODE_PRIVATE);
							outputStream.write(in_node.value.getBytes());
							outputStream.close();
							Log.v(TAG,"iKey "+in_node.key+" Inserted in "+in_node.port+ "hash = "+genHash(in_node.key));
						} catch (Exception e) {
							Log.e(TAG, "File write failed");
						}
						in_node.msg = "Ack10";
						ServerToClient.writeObject(in_node);
						ServerToClient.flush();
					}
					if(in_node.msg.equals("Query")){
						Log.v(TAG,"iKey "+ in_node.query +". Query Message Received on " + in_node.port);
						FileInputStream fis = null;
						try {
							//the parameter 'selection' specifies the criteria for selecting rows which in our case is the key and hence the filename.
							fis = getContext().openFileInput(in_node.query);
							StringBuffer fileContent = new StringBuffer("");
							byte[] buffer = new byte[1024];
							int n;
							while ((n = fis.read(buffer)) != -1)
							{
								fileContent.append(new String(buffer, 0, n));
							}
							in_node.query_result = String.valueOf(fileContent);
							in_node.msg = "Query_value";

							Log.v(TAG,"iKey "+ in_node.query+". Found value " + in_node.query_result + " on " + in_node.port);

							ServerToClient.writeObject(in_node);
							ServerToClient.flush();
						} catch (FileNotFoundException e) {
							Log.e(TAG, "iKey FILE NOT FOUND for " + in_node.query + " on " + in_node.port);
						} catch (IOException e){
							Log.e(TAG, "iKey COULD NOT READ THE FILE for " + in_node.query + " on " + in_node.port);
						}
					}
					if(in_node.msg.equals("Query All") || in_node.msg.equals("Recovering")) {
						Iterator<String> itr = keys.iterator();
						StringBuffer key = new StringBuffer();
						StringBuffer value = new StringBuffer();
						while (itr.hasNext()) {
							//Log.v(TAG, "Special Parameter");
							FileInputStream fis = null;
							String sel = itr.next();
							//Log.v(TAG, sel);
							try {
								fis = getContext().openFileInput(sel);
							} catch (FileNotFoundException e) {
								Log.e(TAG, "FILE NOT FOUND");
								//e.printStackTrace();
							}
							StringBuffer fileContent = new StringBuffer("");

							byte[] buffer = new byte[1024];
							int n;
							try {
								while ((n = fis.read(buffer)) != -1) {
									fileContent.append(new String(buffer, 0, n));
								}
							} catch (Exception e) {
								Log.e(TAG, "COULD NOT READ THE FILE");
								//e.printStackTrace();
							}
							key.append(sel);
							key.append("-");
							value.append(String.valueOf(fileContent));
							value.append("-");
							//String[] rows = {sel, String.valueOf(fileContent)};
						}
						in_node.query = String.valueOf(key);
						in_node.query_result = String.valueOf(value);
						if(in_node.msg.equals("Query All")){
							in_node.msg = "Ack*";
							ServerToClient.writeObject(in_node);
							ServerToClient.flush();
						}
						else if(in_node.msg.equals("Recovering")){
							Log.v(TAG,"Recoverd all messages "+in_node.port);
							in_node.msg = "Recovering Ack";
							ServerToClient.writeObject(in_node);
							ServerToClient.flush();
						}
					}
					if(in_node.msg.equals("Delete All")){
						Iterator<String> itr = keys.iterator();
						while(itr.hasNext()){
							File file = new File(itr.next());
							file.delete();
						}
						keys.clear();
						in_node.msg = "Delete*";
						ServerToClient.writeObject(in_node);
						ServerToClient.flush();
					}
					if(in_node.msg.equals("Delete")){
						File file = new File(in_node.del);
						file.delete();
						keys.remove(in_node.del);
						in_node.msg = "Deleted";
						ServerToClient.writeObject(in_node);
						ServerToClient.flush();
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
				Log.e(TAG, "Unknown Exception");
			}
			return null;
		}
	}
	private class ClientTask extends AsyncTask<Node, Void, Void> {
		@Override
		protected Void doInBackground(Node... nodes){
			Node n = nodes[0];
			if(n.msg.equals("Insert") || n.msg.equals("Query") || n.msg.equals("Delete")){
                /*
			    If the message type is "Insert", "Query" or "delete" just send the message to the post number set in the Message object.
			    */
				//Log.v(TAG,"Instructing "+n.port+" to insert"+n.key);
				try{
					Socket socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							parseInt(n.port));
					ObjectOutputStream objToServer2 = new ObjectOutputStream(socket2.getOutputStream());
					ObjectInputStream objFromServer2 = new ObjectInputStream(socket2.getInputStream());
					objToServer2.writeObject(n);
					objToServer2.flush();
					Node ack = null;
					try {
						ack = (Node) objFromServer2.readObject();
					} catch (ClassNotFoundException e) {
						e.printStackTrace();
					}
                    //If the acknowledgement of the insert message is received, close all the streams and close the socket.
					if (ack.msg.equals("Ack10") && ack.key.equals(n.key)) {
						//Log.v(TAG,"Message: "+ack.msg);
						objToServer2.close();
						objFromServer2.close();
						socket2.close();
						//Log.v(TAG,"Closing all the resources");
					}
                    //If the acknowledgement of the query message is received, store the value of the key queried in 'query_results'.
                    //Also close all the streams and close the socket.
					if (ack.msg.equals("Query_value") && ack.query_result != null){
						query_results.put(ack.query,ack.query_result);
						//Log.v(TAG,"Value for key "+ack.query+" is "+query_results.get(ack.query));
						objToServer2.close();
						objFromServer2.close();
						socket2.close();
					}
                    //If the acknowledgement of the delete message is received, close all the streams and close the socket.
					if(ack.msg.equals("Deleted")){
						objToServer2.close();
						objFromServer2.close();
						socket2.close();
					}
				} catch (UnknownHostException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if(n.msg.equals("Query All") || n.msg.equals("Delete All")){
				//Iterator<Node> iter = node_list.iterator();
				Set set = node_list.entrySet();
				Iterator iter = set.iterator();
				while (iter.hasNext()){
					Map.Entry me = (Map.Entry)iter.next();
					String current_port = (String) me.getValue();
					try{

						Socket socket3 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								parseInt(current_port));
						ObjectOutputStream objToServer3 = new ObjectOutputStream(socket3.getOutputStream());
						ObjectInputStream objFromServer3 = new ObjectInputStream(socket3.getInputStream());
						objToServer3.writeObject(n);
						objToServer3.flush();
						Node ack = null;
						try {
							ack = (Node) objFromServer3.readObject();
						} catch (ClassNotFoundException e) {
							e.printStackTrace();
						}
						if (ack.msg.equals("Ack*")){
							String[] local_keys = ack.query.split("-");
							String[] local_values = ack.query_result.split("-");
							for(int i=0; i<local_keys.length; i++){
								query_results.put(local_keys[i],local_values[i]);
							}
							objToServer3.close();
							objFromServer3.close();
							socket3.close();
						}
						if (ack.msg.equals("Delete*")){
							objToServer3.close();
							objFromServer3.close();
							socket3.close();
						}
					}catch (UnknownHostException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}

			}
			if(n.msg.equals("Recovering")){
				//Log.v(TAG,"Trying to recover messages from "+n.port);
				Set set = node_list.entrySet();
				Iterator iter = set.iterator();
				while (iter.hasNext()){
					Map.Entry me = (Map.Entry)iter.next();
					String current_port = (String) me.getValue();
					Log.v(TAG,"Trying to recover messages from "+current_port);
					try{
						Socket socket4 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								parseInt(current_port));
						socket4.setSoTimeout(200);
						ObjectOutputStream objToServer4 = new ObjectOutputStream(socket4.getOutputStream());
						ObjectInputStream objFromServer4 = new ObjectInputStream(socket4.getInputStream());
						n.port = current_port;
						objToServer4.writeObject(n);
						objToServer4.flush();
						Node ack = new Node();
						try {
							ack = (Node) objFromServer4.readObject();
						} catch (ClassNotFoundException e) {
							e.printStackTrace();
						}
						if (ack.msg.equals("Recovering Ack")){
							Log.v(TAG,"Recovered messages from "+ack.port);
							String[] local_keys = ack.query.split("-");
							String[] local_values = ack.query_result.split("-");
							for(int i=0; i<local_keys.length; i++){
								recovery_results.put(local_keys[i],local_values[i]);
							}
							objToServer4.close();
							objFromServer4.close();
							socket4.close();
						}
					} catch(SocketTimeoutException e){
						Log.e(TAG,"Socket Timed Out");
						continue;
					}
					catch (UnknownHostException e) {
						//e.printStackTrace();
					} catch (StreamCorruptedException e) {
						//e.printStackTrace();
					} catch (IOException e) {
						//e.printStackTrace();
					}
				}
				for (Map.Entry<String, String> entry : recovery_results.entrySet()) {
					String key = entry.getKey();
					String hash_file = new String();
					String correct_port = new String();
					String replica1_port = new String();
					String replica2_port = new String();
					String correct_hash = new String();
					String replica1_hash = new String();
					String replica2_hash = new String();
					try {
						hash_file = genHash(key);
					} catch (NoSuchAlgorithmException e) {
						e.printStackTrace();
					}
					//get_info.hash = hash_file;
					//Node correct = new Node();
					//Node replica1 = new Node();
					//Node replica2 = new Node();
					if(hash_file.compareTo(node_list.lastKey()) > 0){
						correct_hash = node_list.firstKey();
						correct_port = node_list.get(correct_hash);
					}
					else {
						correct_hash = node_list.ceilingKey(hash_file);
						correct_port = node_list.get(correct_hash);
					}
					replica1_hash = node_list.higherKey(correct_hash);
					if(replica1_hash == null){
						replica1_hash = node_list.firstKey();
						replica1_port = node_list.get(replica1_hash);
					}
					else{
						replica1_port = node_list.get(replica1_hash);
					}
					replica2_hash = node_list.higherKey(replica1_hash);
					if(replica2_hash == null){
						replica2_hash = node_list.firstKey();
						replica2_port = node_list.get(replica2_hash);
					}
					else{
						replica2_port = node_list.get(replica2_hash);
					}
					if(myAvdPort.equals(correct_port) || myAvdPort.equals(replica1_port) || myAvdPort.equals(replica2_port)){
						keys.add(key);
						String value = entry.getValue();
						FileOutputStream outputStream;
						try {
							outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
							outputStream.write(value.getBytes());
							outputStream.close();
							Log.v(TAG,"rKey "+key+" Inserted in "+myAvdPort+ "hash = "+genHash(key));
						} catch (Exception e) {
							Log.e(TAG, "File recovery failed");
						}
					}
				}
				recovery_results.clear();
			}
			return null;
		}
	}
}
