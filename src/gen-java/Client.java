
import java.util.Scanner;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TSSLTransportFactory;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TSSLTransportFactory.TSSLTransportParameters;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;


public class Client {

	public static int getHashcode(String url){
		int key = url.hashCode();
		if(key<=0){
			key = key >>>1;
		}
		return key;
		
	}
	public static void main(String[] args){
		
		String host = "localhost";
		int port = 9002;
		
		String word ;
		int choice =1;
		Scanner sc = new Scanner(System.in);
		while(choice==1){
			System.out.println("Enter 1 to lookup , 2 to exit");
			
			System.out.println("Enter your Choice : ");
			choice = sc.nextInt();
			if(choice==1){
				System.out.println("Enter a Word : 1");
				word = sc.next();
				System.out.println("USer entered "+ word);
				String targeturl="";
				
				try{ 
					
					TTransport transport;
	  		        transport = new TSocket(host, port);
	  		        transport.open();
	  		        TProtocol protocol = new  TBinaryProtocol(transport);
	  				AddService.Client client = new AddService.Client(protocol);
					
	  				targeturl = client.find_node(getHashcode(word) , true);
				}
				catch(TException e){
					
				}
				
  				String targetHost = targeturl.split(":")[0];
				int targetPort = Integer.parseInt(targeturl.split(":")[1].split("/")[0]);
				
				try{
					TTransport transport2;
	  		        transport2 = new TSocket(targetHost, targetPort);
	  		        transport2.open();
	  		        TProtocol protocol2 = new  TBinaryProtocol(transport2);
	  				AddService.Client client2 = new AddService.Client(protocol2);
	  				String meaning = client2.lookup(word);	
	  				System.out.println(" Result : "+ meaning);
					
				}
				catch(TException e ){
					System.out.println("Error in lookup");
				}
  				
				
				
			}else{
				System.out.println("Exiting.. ");
			}
		}
	}
}
