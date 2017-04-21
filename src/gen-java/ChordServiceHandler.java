import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;


class ChordServiceHandler implements AddService.Iface
{
	public static int keyLength = 32;
	private int maxkeyval = (int)Math.pow(2, 32);
	private int nodeID;
	private String hostName;
	private int port;
	private int hashedKey;
	private String URL;
	private Lock nodeLock;
	private Node predecessor;
	private Node successor;
	// dictionary storage
	Map<Integer,String> wordMap ;     //= new HashMap<String,String>();  

	// finger table stores the finger node number (i) and URL of the ith finger node 
	Map<Integer,Node> fingerTable  ;     //= new HashMap<Integer,Node>();

	private PrintWriter fw;

	public String find_node(int key, boolean traceflag){
		Node snode = this.find_successor(key);
		return snode.getURL();
		
	}
	
	public String lookup (String word){
		int wordkey = getHashcode(word);
		if(wordMap.containsKey(wordkey)){
			return wordMap.get(wordkey);
		}
		else 
			return "NOT FOUND";
		
	}
	
	
	public int getHashcode(String url){
		int key = url.hashCode();
		if(key<=0){
			key = key >>>1;
		}
		return key;
		
	}
	
	public void setHashedKey(int id){
		this.hashedKey = id;
	}
	
	public boolean insert (String word, String meaning){
		int wordkey = getHashcode(word); //word.hashCode();
		this.wordMap.put(wordkey, meaning);
		return true;
	}
	
	
	public ChordServiceHandler(String URL)
	{
		String[] urlparts = URL.split(":");
		String host = urlparts[0];
		int tport = Integer.parseInt(urlparts[1].split("/")[0]);
		int tnodeId = Integer.parseInt(urlparts[1].split("/")[1]);
		
		this.nodeID = tnodeId;
		this.URL = URL;
		this.hashedKey =  getHashcode(this.URL);
		this.predecessor = null;
		this.successor = null;
		this.port = tport;
		this.hostName = host;
	   	wordMap  = new HashMap<Integer,String>(); 
		fingerTable = new HashMap<Integer,Node>();
		
		try {	
			fw=new PrintWriter("Node"+nodeID+"LogFile");
		} catch (IOException e) {
			//e.printStackTrace();
		}
	}

	public void printFingerTable(){
		fw.append("printing fingerTable..\n");
		fw.flush();
		fw.append("FingerNo        FingerNodeURL");
		fw.flush();
		for(Map.Entry<Integer,Node> m : fingerTable.entrySet()){

			fw.append(m.getKey()+"       "+m.getValue().getURL() );
			fw.flush();
		}

	}

	public Node getSuccessor(){
		return this.successor;
	}
	
	public Node getPredecessor (){
		return this.predecessor;
	}
	
	public void setSuccessor(String url){
		String[] urlparts = url.split(":");
		String host = urlparts[0];
		int port = Integer.parseInt(urlparts[1].split("/")[0]);
		int nodeId = Integer.parseInt(urlparts[1].split("/")[1]);
		Node node = new Node( url, host, port, getHashcode(url) );
		this.successor = node;
	}
	
	public void setPredecessor(String url){
		String[] urlparts = url.split(":");
		String prehost = urlparts[0];
		int preport = Integer.parseInt(urlparts[1].split("/")[0]);
		int pnodeId = Integer.parseInt(urlparts[1].split("/")[1]);
		Node node = new Node(url, prehost, preport, getHashcode(url)  );
		this.predecessor = node;
	}
	
	public Join_data join(String url){   // same functionality as init_finger_table code in algorithm
		nodeLock.lock();
		try{
			String[] urlparts = url.split(":");
			String newnodehost = urlparts[0];
			int newnodeport = Integer.parseInt(urlparts[1].split("/")[0]);
			int newnodeId = Integer.parseInt(urlparts[1].split("/")[1]);
			
			Map<Integer,Node> temp_fingerTable = new HashMap<Integer,Node>();
			
			Node newnodeSuccessor = null;
			Node newnodePredecessor = null;
			Node newnode = null;
			int newnodehashKey = getHashcode(url) ; //url.hashCode();
			
			Node finger1 = this.find_successor(newnodehashKey+1);
			
			//newnode = new Node(newnodeId, url);
			
			
			newnodeSuccessor  =finger1;  //= new Node ( Integer.parseInt(urlparts[1].split("/")[1]), finger1.split(":")[0]);
			
			String host = newnodeSuccessor.getHostName();
			int port = newnodeSuccessor.getPort();
			try{
			TTransport transport; 
		    transport = new TSocket(host, port);
		    transport.open();
		    TProtocol protocol = new  TBinaryProtocol(transport);
		    AddService.Client client = new AddService.Client(protocol);
		   
		    newnodePredecessor = client.getPredecessor();
			
		    //client here is the successor of the newly inserted node
		    
		    client.setPredecessor(url);   // set the newly inserted node as the predecessor of its successor
		    
			}catch(TException E){
		    
			}
		    //Node tempnode = 
		    
		    temp_fingerTable.put(1, finger1);
		    for ( int i =1;i<keyLength;i++){
		    	
		    	if(fingerStart(newnodehashKey, i+1) >= this.hashedKey && fingerStart(newnodehashKey,i+1)<temp_fingerTable.get(i).getKey()){
		    		temp_fingerTable.put(i+1, temp_fingerTable.get(i));
		    	}
		    	else{
		    		Node  temp = find_successor(fingerStart(newnodehashKey,i+1)); // find_successor and find_node are same
		    		temp_fingerTable.put(i+1, temp);
		    	
		    	}
		    }
		    
		    Join_data return_info = new Join_data(newnodehashKey, temp_fingerTable, newnodeSuccessor, newnodePredecessor);
		  //  i feel its better if this call is made by new node which joined the network..      
		   this.updateOthers(newnode); 
		   return return_info;
		}
		finally{
			nodeLock.unlock();
		}

	}
	
	
	public void updateOthers(Node n){
		for (int i =1 ;i<=	keyLength;i++){
			//this could be parallelized by using other node to call find_predecessor
			Node p = this.find_predecessor((n.getKey()- ((int)Math.pow(2,i-1)%maxkeyval)));
			try{
				TTransport transport; 
				transport = new TSocket(p.getHostName(), p.getPort());
				transport.open();
				TProtocol protocol = new  TBinaryProtocol(transport);
				AddService.Client client = new AddService.Client(protocol);
				client.update_finger_table(n, i);
			}
			catch(TException e){
				
			}
			
			
		}
	}
	
	public void update_finger_table(Node s,  int i)   {
		//s here is the new node 
		
		if(s.getKey() >=this.hashedKey &&  s.getKey()< this.fingerTable.get(i).getKey()){
			this.fingerTable.put(i, s);
			
			Node p = this.predecessor;   // first node preceding the current node
		
			try{
				TTransport transport; 
				transport = new TSocket(p.hostName, p.port);
				transport.open();
				TProtocol protocol = new  TBinaryProtocol(transport);
				AddService.Client client = new AddService.Client(protocol);
				
				client.update_finger_table(s ,i);
			}catch(TException e){
				
			}
			
		}
		//return true;
	}
	
	
	public int fingerStart(int fromkey,  int fingernumber ){
		return (fromkey + ((int)Math.pow(2, fingernumber-1) % maxkeyval  ));
	}

	
	
	public Node closest_preceding_finger(int id){
		for( int i =keyLength; i>0;i++){
			int ithfingerid = this.fingerTable.get(i).getKey();
			if( ithfingerid > this.hashedKey && ithfingerid <= id ){
				return this.fingerTable.get(i);
			}
		}
		
		
		Node node = new Node( this.URL, this.hostName, this.port, this.hashedKey) ;
		return node;
	}
	
	
	
   public Node find_predecessor(int id){
	
	   int temphashid = id;
	   String host;
	   Node nprime=null;
	   if(temphashid>this.hashedKey && temphashid<= this.successor.getKey() ){
		  return new Node( this.URL, this.hostName, this.port, this.hashedKey) ;
	   }
	   else{
		   nprime = closest_preceding_finger(id);
	   }
	   while(!(temphashid>nprime.getKey() && temphashid<= this.find_successor(nprime.getKey()).getKey() )){
		   host = nprime.hostName;
		   port = nprime.port;
		   try{
			   TTransport transport; 
			   transport = new TSocket(host, port);
			   transport.open();
			   TProtocol protocol = new  TBinaryProtocol(transport);
			   AddService.Client client = new AddService.Client(protocol);
			   nprime = client.closest_preceding_finger(id);
			   
		   }catch(TException e){
			   
		   }
		  
	   }
	   return nprime;
   }
	
  
   
   public Node find_successor(int id){
		Node nprime = find_predecessor(id);
		//String host = p.hostName;
		//int port = p.port;
		
		/*TTransport transport; 
	    transport = new TSocket(this.hostName, this.port);
	    transport.open();
	    TProtocol protocol = new  TBinaryProtocol(transport);
	    AddService.Client client = new AddService.Client(protocol);
	    Node nprime = client.find_predecessor(url);
	   */ 
	    try{
	    	TTransport transport; 
		    transport = new TSocket(nprime.hostName, nprime.port);
		    transport.open();
		    TProtocol protocol = new  TBinaryProtocol(transport);
		    AddService.Client clientnprime = new AddService.Client(protocol);
		    return clientnprime.getSuccessor();
	    }catch(TException e){
	    	fw.append("TException occured while finding successor");
	    	return nprime;
	    	
	    }
	    
	    
	}
	
   
   
}