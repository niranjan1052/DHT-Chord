struct Node {

1:   string URL;
2:   string hostName;
3:   i32 port;
4:   i32 key;
}



struct Join_data{

 1:  i32 id;
 2:  map<i32, Node> fingerTable	;
 3:  Node successor;
 4:  Node predecessor;


}




service AddService {



   //String lookup(1:string word);
  // i32 insert(1:string word);
  // void printFingerTable();

  // String find_node(1:i32 key, 2: bool traceflag);

   void update_finger_table(1: Node s, 2: i32 i)	;

   Node find_successor(1 : i32 id);

   Node find_predecessor(1 : i32 id);

   Node closest_preceding_finger(1:i32 id);

   Join_data join(1:string url);

   Node getSuccessor();

   Node getPredecessor();

   void setSuccessor(1:string url);

   void setPredecessor(1:string url);

   void printFingerTable();

   string find_node(1: i32 key , 2: bool traceflag);

   string lookup(1: string word);

   bool insert (1: string word, 2: string meaning);


}
