/**************************************************************************************************************
*************************Chord Protocol Implementation*************************************************************
*Instuctions to Run: 
*Execute the following command from SBT bild directory: sbt "project project3" "run <num of nodes><num of request>" 
*Publisher: Souav kumar parmar 
*           Priyanshu Pandey
****************************************************************************************************************/
import akka.actor.{Actor, ActorRef, ActorSystem, Props, PoisonPill}
import java.security.MessageDigest
import scala.util.Random
import scala.collection.mutable.HashMap
import akka.util.Timeout
import scala.collection.immutable.{TreeMap, List}

case class m_FirstJoin(numrequest : Int,numnodes : Int)
case class m_Join(asknode:ActorRef)
case class m_locateposition(node:ActorRef,nodeid:Long)
case class m_nodePosLocated(predecessor:ActorRef,successor:ActorRef)
case class Find_Finger_Entry(node:ActorRef,i:Int,start:Long)
case class Found_Finger_Entry(i:Int,successor:ActorRef)
case class Update_Finger_Entry(before:Long,i:Int,node:ActorRef,nodeid:Long)
case class m_setPredecessor(node:ActorRef)
case class m_setSuccessor(node:ActorRef)
case class Find(node:ActorRef,key:Long, hop:Int, msgcount:Int)
case class Found(key:Long,predecessor:ActorRef,successor:ActorRef,hop:Int,msgnumber:Long)

object Global {
    var nodemap = new HashMap[Long, ActorRef]
     val max_len =24
     val m_maxnodes:Long = math.pow(2,max_len).toLong
     }



object Main extends App{
     if(args.length == 2) {
       var m_numofnodes = args(0).toInt
       var m_numRequest = args(1).toInt
       val system = ActorSystem("Chordsimulator")
       var begin = system.actorOf(Props(new ChordNetwork(m_numofnodes, m_numRequest)),"ChordNetwork")
       begin ! "createNetwork"
     }
     else
       println("Input in format should be : <numofnodes> <numRequest>")
  }

/********************************************************************************************
*class ChordNetwork: Works as master worker 
*    createNetwork: Creates the network by joining nodes on at a time.
*    Send_Messages: Initiates message lookup once the network is created.
*    consistenthash: Calculates the SHA1 hash and truncates it to m bits.
 ********************************************************************************************/
  class ChordNetwork(numofnodes:Int , numRequest: Int ) extends Actor {
    var numm_Joined:Int = 0
    var nodeID : Long = 0
    var node:ActorRef = null
    var retrycount:Int = 0   
    var refNode : ActorRef = null
    var nodeset = scala.collection.mutable.Set[Long]()
    def receive = {
      case "createNetwork"=>{
        println("Network create initiating \n") 

        val system1 = ActorSystem("Worker")
                  while(nodeset.size != numofnodes)
                  {
                     nodeset += consistenthash(Random.nextInt(2000000))
                  }
                  for( i<-0 until numofnodes){  
                     try { 
                            nodeID = nodeset.toVector(i)
                            node = (system1.actorOf(Props(new Peer(nodeID)),name = getmyname(nodeID))) 
                                         // ...
                      } catch {
                        case e: Exception => {
                          if(retrycount < 10){
                            println("Actor name clash trying again")
                            nodeID = consistenthash(Random.nextInt(2000000))
                            node = (system1.actorOf(Props(new Peer(nodeID)),name = getmyname(nodeID))) 
                            retrycount = retrycount +1
                          }
                      }                   
                     }
                     if(i==0)//First Node added to network. This node will act as the bootstrap node.
                        {
                          refNode = node
                          refNode ! m_FirstJoin(numRequest,numofnodes)
                        }
                      else
                        {    
                         node ! m_Join(refNode)  
                            Thread.sleep(100) 
                         } 
                    Global.nodemap.put(nodeID,node)
                  }
      }
      
      case "m_Joined" => {
           numm_Joined = numm_Joined + 1
            if(numm_Joined == numofnodes - 1){
             // println("m_Joined " + numm_Joined)
                  Send_Messages()// All nodes have been added to the network. Initiate message passing.

        }
      }
    }         
  def Send_Messages() ={
            var msgsent:Int =0
            for( i<-0 until numRequest)
            { 
              for(j<-0 until numofnodes)
              {   msgsent = msgsent +1
                 nodeID = nodeset.toVector(j)
                 node = Global.nodemap(nodeID)
                 val msg:Long = consistenthash(Random.nextInt(1000000))
                //  println("Send_Messages to"+node)
                  Thread.sleep(30)
                 node ! Find(node,msg,0,msgsent)
              }
                 
            }
        }

   def consistenthash(index:Int): Long={
      var index1:String = index.toString
      var sha:String = ""
      sha = MessageDigest.getInstance("SHA-1").digest(index1.getBytes("UTF-8")).map("%02x".format(_)).mkString

     val res:Long = Parsefirstmbits(sha)
     return res
 }
   def getmyname (nodeId: Long):String ={
    return nodeId.toString
   }
   def Parsefirstmbits(sha:String):Long={
    val loop:Int = 6 
    var res: Long = 0
    for(i<-0 to loop-1)
       res = (res << 4 ) | Character.digit(sha.charAt(i), 16);
    res = res & 0xFFFFFFFF 
    return res
   }

 }

 /********************************************************************************************
*class ChordNetwork: Works as master worker 
*m_FirstJoin : Initializes the class members for the first node.
*m_Join: Adds a new node by requesting the bootstrap node.
*m_locateposition: Finds the predecessor and successor of a newly add node.
*m_nodePosLocated : Notified once successor and predecessor are successfully located for a newly joined node.
*m_setPredecessor: Set predecessor of crrent node
*m_setSuccessor: Set predecessor of crrent node
*Find: Finds the key in in the peers.
*Found: Reports the Average hop count.
 ********************************************************************************************/

class Peer(nodeID : Long) extends Actor{
  var fingerTable = new Array[FingerProp](Global.max_len) 
     var Successor:ActorRef = self
     var Predecessor:ActorRef = self
     var refNode : ActorRef=null
     var m_maxnodes : Long = Global.m_maxnodes
     val m : Int = Global.max_len
     var hopcount : Int = 0
     var numRequest: Int = 0
     var numofnodes : Int = 0

  fingertableinit()
  def My_NodeID():Long={
          val pattern = "([0-9]+)".r
          val pattern(num) = self.path.name
          //println(num.toInt)
          return num.toInt
  }
  def My_NodeID(node:ActorRef):Long={
          val pattern = "([0-9]+)".r
          val pattern(num) = node.path.name
          //println(num.toInt)
          return num.toInt
  }

  def closest_preceding_finger(id:Long): ActorRef = {
    val interval=new Interval(false,My_NodeID(),id,false)
    for(i <- m-1 to 0 by -1){
      if(interval.inValid(fingerTable(i).getNodeID())) {
        return fingerTable(i).node;
      }
    }
    return self;
  }

  def init_finger_table():Unit = {
    fingerTable(0).setNode(Successor)
    fingerTable(0).setNodeID(My_NodeID(Successor))
      //  println("Node    " +self+"init_finger_table Entry at 0"+Successor)

    for(i<-0 until m-1){
      val interval=new Interval(true,My_NodeID(),fingerTable(i).getNodeID(),true)
      if(interval.inValid(fingerTable(i+1).getStart())) {
           //println("Node    " +self +"init_finger_table Entry at "+i+"is" +fingerTable(i).getNode())
        fingerTable(i+1).setNode(fingerTable(i).getNode())
        fingerTable(i+1).setNodeID(My_NodeID(fingerTable(i).getNode()))
      }
      else{
        if(refNode!=null){
          refNode!Find_Finger_Entry(self,i+1,fingerTable(i+1).getStart())
        }
      }
    }
  }

  def update_others():Unit = {
    for(i <- 0 to m-1) {
      val position=(My_NodeID()-(math.pow(2,i).toLong)+(math.pow(2,m)).toLong+1)% m_maxnodes
      Successor!Update_Finger_Entry(position,i,self,My_NodeID())
    }
  }

 def fingertableinit ():Unit={
  for(i <-0 until m) {
    val start=(My_NodeID()+(math.pow(2, i)).toLong) % m_maxnodes
    val end=(My_NodeID()+(math.pow(2, i+1) ).toLong)% m_maxnodes
    val interval= new Interval(true, start,end, false)
    fingerTable(i)= new FingerProp(start,interval,self,nodeID)
  }
}

  override def receive: Receive ={
    case m_FirstJoin(numrequest : Int,numnodes : Int)=>{
      this.numRequest = numrequest
      this.numofnodes = numnodes
      refNode = self
      sender ! "m_Joined"
    }

    case m_Join(asknode:ActorRef)=>{
    //  println("m_Join")
      this.refNode=asknode
      refNode!m_locateposition(self,My_NodeID)
      sender ! "m_Joined"
    }

    case m_nodePosLocated(predecessor:ActorRef,successor:ActorRef)=>{
      this.Predecessor=predecessor
      this.Successor=successor
      Predecessor!m_setSuccessor(self)
      Successor!m_setPredecessor(self)
      println(" Position of New Node found -->"+My_NodeID(self)+" Successor is "+My_NodeID(Successor)+ " and  Predecessor is"+ My_NodeID(Predecessor))
      init_finger_table()
      //println("Finger Initiated")
      update_others()
     // println("Others Updated")
      sender ! "m_Joined"
    }

    case Found_Finger_Entry(i:Int,successor:ActorRef)=>{
     // println("Node    " +My_NodeID(self) +"Found_Finger_Entry Entry at "+i+ "is "+My_NodeID(successor))
      this.fingerTable(i).setNode(successor)
      this.fingerTable(i).setNodeID(My_NodeID(successor))
    }

    case m_locateposition(node:ActorRef,nodeid:Long)=>{
      val interval = new Interval(false, My_NodeID(), fingerTable(0).getNodeID(), true)
      if(interval.inValid(nodeid)){
        node!m_nodePosLocated(self,this.Successor)
      }else{

        val target=closest_preceding_finger(nodeid)
        target!m_locateposition(node,nodeid)
      }
    }

    case Find_Finger_Entry(node:ActorRef,i:Int,start:Long)=>{
      val interval = new Interval(false, My_NodeID(), fingerTable(0).getNodeID(), true)
      if(interval.inValid(start)){
        node!Found_Finger_Entry(i,Successor)
      }else{
        val target=closest_preceding_finger(start)
        target!Find_Finger_Entry(node,i,start)
      }
    }

    case Update_Finger_Entry(before:Long,i:Int,node:ActorRef,nodeid:Long)=>{
      if(node!=self) {
        val interval1 = new Interval(false, My_NodeID(), fingerTable(0).getNodeID(), true)
        val interval2=new Interval(false, My_NodeID(), fingerTable(i).getNodeID(), false)
        if (interval1.inValid(before)) {
            if(interval2.inValid(nodeid)){
              fingerTable(i).setNode(node)
              fingerTable(i).setNodeID(My_NodeID(node))
              Predecessor!Update_Finger_Entry(My_NodeID(),i,node,nodeid)
            }
        }else{
          val target=closest_preceding_finger(before)
          target!Update_Finger_Entry(before,i,node,nodeid)
        }
      }
    }

    case m_setPredecessor(node:ActorRef)=>{
      this.Predecessor=node
    }

    case m_setSuccessor(node:ActorRef)=>{
      this.Successor=node
    }

    case Find(node:ActorRef,key:Long, hop:Int, msgcount:Int)=>{
      var interval = new Interval(false, My_NodeID(Predecessor), My_NodeID(), true)
      val interval2 = new Interval(false, My_NodeID(), fingerTable(0).getNodeID(), true)

      if(interval.inValid(key))
      {
        println("Msg "+ msgcount +" Routed Key located at "+My_NodeID())
        refNode!Found(key,Predecessor,self,hop,msgcount)
      }
      else if(interval2.inValid(key))
      {
        println("Msg "+ msgcount +" Routed Key located at "+My_NodeID(Successor))
        refNode!Found(key,self,Successor,hop + 1,msgcount)
      }
     else
      {
        val target=closest_preceding_finger(key)
        println("------Msg " + msgcount+" Still Routing Sent to "+My_NodeID(target))
        target!Find(node,key,hop + 1,msgcount)
      }
    }

    case Found(key:Long,predecessor:ActorRef,successor:ActorRef,hop:Int,msgnumber:Long)=>{
         // println("Destination of Key "+key+" found at "+My_NodeID(successor))
          hopcount = hopcount + hop
          if(msgnumber == numRequest*numofnodes)
          {
           var finalaverage :Double = hopcount.toDouble/msgnumber.toDouble
            println("**************************************************************")
            println("**************************************************************")
            println("All the messages sucessfully routed to their destination.")
            println("Number of Requests for each node  "+numRequest+"   Total number of nodes  "+numofnodes)
            println("")
            println("Average Hop Count is **************** "+finalaverage+" *******************")
            println("**************************************************************")
            println("**************************************************************")
          }
    }
  }
}

class FingerProp(start: Long, interval: Interval, var node:ActorRef,var nodeID:Long){
  def getStart(): Long = {
    return this.start
  }
  def getInterval(): Interval = {
    return this.interval
  }
  def getNode(): ActorRef = {
    return this.node
  }
  def setNodeID(newnodeid :Long):Unit={
     this.nodeID = newnodeid
  }
  def getNodeID():Long={
    return this.nodeID
  }
  def setNode(newNode:ActorRef):Unit ={
    this.node=newNode
  }
}


 /********************************************************************************************
*class ChordNetwork: Works as master worker 
*inValid : Checks if a given nodeid lies is in the given interval.
 ********************************************************************************************/

class Interval(includeStart:Boolean,start:Long,end:Long, includeEnd:Boolean){
  def inValid(nodetobefound:Long): Boolean = { 
    if(start.equals(end))
    {
      if(includeStart.equals(false) && includeEnd.equals(false) && end.equals(nodetobefound))
      {
        return false
      }
      else
      {
        return true
      }
    }
    else if(end > start)
    {
       if(((start < nodetobefound) && (end > nodetobefound)) 
           || (end.equals(nodetobefound)&&includeEnd.equals(true)) 
           || (start.equals(nodetobefound) && includeStart.equals(true)))
       {
          return true
       }
       else
          return false
    }
    else if(end < start)
    {
      if(((start < nodetobefound && nodetobefound < Global.m_maxnodes) || (0 <= nodetobefound  && nodetobefound <= end))
          || (start.equals(nodetobefound) && includeStart.equals(true)) 
          || (end.equals(nodetobefound) && includeEnd.equals(true)))
       {
          return true
       }
       else
          return false
    }
    else return false
  } 

  def getEnd(): Long ={
    return end
  }
}
