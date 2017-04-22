import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.*;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TSSLTransportFactory;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TSSLTransportFactory.TSSLTransportParameters;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;

public class DictionaryLoader {

	public static int getHashcode(String url){
		int key = url.hashCode();
		if(key<0){
			key = key >>>1;
		}
		return key;

	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Map<String,String> dictionary = new HashMap<String,String>();
		String inputfile = args[0];
		int noOfNodes = 8;

		// reading class file
		try{
			File class_file = new File(inputfile);
			BufferedReader br = new BufferedReader(new FileReader(class_file));

			String line = null;
			String word ,meaning;
			String classname;
			int classcount=0;
			int k =1;
			while ((line = br.readLine()) != null) {
				StringTokenizer st = new StringTokenizer(line,":");
				word = st.nextToken();
				meaning = st.nextToken();
				dictionary.put(word, meaning);
				// using dictionary we may loose duplicate entries  or may be thats fine
				//as it would have same effect on server side as well
				System.out.println(word+" ---------- "+ meaning);

				String host = "localhost";
				int port = 9001;

				TTransport transport;
  		        transport = new TSocket(host, port);
  		        transport.open();
  		        TProtocol protocol = new  TBinaryProtocol(transport);
  				AddService.Client client = new AddService.Client(protocol);
				String targeturl = client.find_node(getHashcode(word) , true);

				String targetHost = targeturl.split(":")[0];
				int targetPort = Integer.parseInt(targeturl.split(":")[1].split("/")[0]);


				TTransport transport2;
  		        transport2 = new TSocket(targetHost, targetPort);
  		        transport2.open();
  		        TProtocol protocol2 = new  TBinaryProtocol(transport2);
  				AddService.Client client2 = new AddService.Client(protocol2);
  				client2.insert(word, meaning);

			}

			br.close();

		}catch(Exception e){
			System.out.println("Class file not found");
		}


	}

}
