import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.io.PrintWriter;

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


@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
public class Server
{

	public static int getHashcode(String url){
		int key = url.hashCode();
		if(key<=0){
			key = key >>>1;
		}
		return key;

	}

  public static ChordServiceHandler handler;
  public static AddService.Processor processor;

  public static void main(String [] args)
  {

	final String host = args[0];
	final int port=Integer.parseInt(args[1]);
	final int nodeID = Integer.parseInt(args[2]);

	String node0host = "localhost";
	int node0port = 9000;
	String url = host+":"+port+"/"+nodeID;
	PrintWriter fw = null;
	System.out.println(url);

	int hashcode = getHashcode(url);
	try {
		fw=new PrintWriter("server"+nodeID+"LogFile");
	} catch (IOException e) {
		//e.printStackTrace();
	}

    try
    {
      handler = new ChordServiceHandler(url);
      if(nodeID==0){

				fw.append("Printing node data \n");
				Node node = new Node(url,host,port,hashcode);
    	  for(int i=1;i<=handler.keyLength;i++){
					fw.append(" "+ node.getURL()+"\n");
					fw.flush();
    		  handler.fingerTable.put(i,node);
    	  }
				handler.setSuccessor(url);
				handler.setPredecessor(url);
      }else{

    	 try{
    		TTransport transport;
 		    transport = new TSocket(node0host, node0port);
 		    transport.open();
 		    TProtocol protocol = new  TBinaryProtocol(transport);
 		    AddService.Client clientZero = new AddService.Client(protocol);
 		    Join_data joinInfo;
				System.out.println("Server " +nodeID+" calling Join on Node 0");
 		    joinInfo = clientZero.join(url);

				System.out.println("return key "+ joinInfo.getId());
    		//Node node = new Node( url, host, port, joinInfo.getID());
    		handler.setHashedKey(joinInfo.getId());
    		handler.fingerTable = joinInfo.getFingerTable();
    		handler.setSuccessor(joinInfo.getSuccessor().getURL())  ;  					// = joinInfo.getSuccessor();
    		handler.setPredecessor(joinInfo.getPredecessor().getURL())   ;  			// = joinInfo.getPredecessor();

				Node newnode = new Node(url,host,port,hashcode);

				System.out.println("Calling update others");
				handler.updateOthers(newnode);
				System.out.println("Update others completed for node with id "+ nodeID  +" returning joininfo");



    	 }
    	 catch(Exception e){

    	 }

      }

			handler.printFingerTable();

      processor = new AddService.Processor(handler);

      //Runnable simple = new Runnable()
      //{
       // public void run()
       // {
       someMethod(processor, port);
       // }
      //};

      //new Thread(simple).start();



    }
    catch (Exception x)
    {
      //x.printStackTrace();
    }
  }

  public static void someMethod(AddService.Processor processor, int port)
  {
    try
    {
      TServerTransport serverTransport = new TServerSocket(port);
      TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));
      System.out.println("THRIFT SERVER STARTED");

      server.serve();
    }
    catch (Exception e)
    {
      //e.printStackTrace();
    }
  }
}
