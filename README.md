# DHT-Chord

Running instructions

* Navigate to DHT-CHORD/src/gen-java folder
* Compile all the .java files using command 
  javac -cp ".:libs/libthrift-0.9.1.jar:libs/slf4j-api-1.7.12.jar" *.java
* Now to start the servers execute following commands on different terminals

  java -cp ".:libs/libthrift-0.9.1.jar:libs/slf4j-api-1.7.12.jar" Server localhost 9000 0
  i.e. the command line arguements are hostname, port no and node number (0,1,2,3....)                                                     
  java -cp ".:libs/libthrift-0.9.1.jar:libs/slf4j-api-1.7.12.jar" Server localhost 9001 1                                           
  java -cp ".:libs/libthrift-0.9.1.jar:libs/slf4j-api-1.7.12.jar" Server localhost 9002 2
  
  according to the number of servers needed to be joined in the network.
  
* Once All the nodes are joined, Run the DictionaryLoader file                                                                  
  java -cp ".:libs/libthrift-0.9.1.jar:libs/slf4j-api-1.7.12.jar" DictionaryLoader1
 
* Now run the Client program to start lookup operations 
  java -cp ".:libs/libthrift-0.9.1.jar:libs/slf4j-api-1.7.12.jar" Client 
  and follow the instructions on the screen to provide inputs and perform word lookups.
  
  
  // To generate thirft class files Execute Command from the src folder
  thrift -r --gen java AddService.thrift
  
 
 
