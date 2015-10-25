import akka.actor.{Actor, ActorRef, ActorSystem, Props, PoisonPill}
import scala.collection.immutable.{TreeMap, List}
import java.security.MessageDigest
import scala.util.Random
import scala.collection.mutable.HashMap
import akka.util.Timeout
//import java.math.Longeger
import scala.collection.immutable.{TreeMap, List}


case class m_Firstm_Join(nodeid : Long)
case class m_Join(asknode:ActorRef)
case class m_locateposition(node:ActorRef,nodeHash:Long)
case class m_nodePosLocated(predecessor:ActorRef,successor:ActorRef)
case class Find_Finger_Entry(node:ActorRef,i:Int,start:Long)
case class Found_Finger_Entry(i:Int,successor:ActorRef)
case class Update_Finger_Entry(before:Long,i:Int,node:ActorRef,nodeHash:Long)
case class m_FindMsgTarget(node:ActorRef,code:String, Hop:Int)
case class m_FoundMsgTarget(code: String,predecessor:ActorRef,successor:ActorRef,Hop:Int)
case class m_setPredecessor(node:ActorRef)
case class m_setSuccessor(node:ActorRef)
case object Print

object Global {
    var nodemap = new HashMap[Long, ActorRef]
     val max_len =4
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
       println("Input in format <numofnodes> < numRequest>")
  }

  class ChordNetwork(numofnodes:Int , numRequest: Int ) extends Actor {
    var numm_Joined:Int = 0
    var nodeID : Long = 0
    var node:ActorRef = null
    var totalHopCount = 0.0f
    var aggregate = 0.0f
    var averageHopCount = 0.0f 
    var retrycount:Int = 0   
    var refNode : ActorRef = null
    def receive = {
      case "createNetwork"=>{
          println("Network create initiating \n") 

       val system1 = ActorSystem("Worker")
       var nodeset = scala.collection.mutable.Set[Long]()
                  while(nodeset.size != numofnodes)
                  {
                     nodeset += consistenthash(Random.nextInt(2000000))
                   }
                  for( i<-0 to numofnodes-1){  
                     Thread.sleep(100)          
                     try { 
                            nodeID = consistenthash(Random.nextInt(2000000))
                            node = (system1.actorOf(Props(new Peer(nodeID)),name = getmyname(nodeID))) 
                                         // ...
                      } catch {
                        case e: Exception => {
                          if(retrycount < 10){
                            println("Actor name clash trying max try again")
                          nodeID = consistenthash(Random.nextInt(2000000))
                            node = (system1.actorOf(Props(new Peer(nodeID)),name = getmyname(nodeID))) 
                            retrycount = retrycount +1
                          }
                      }                   
                     }
                     if(i==0)
                        {
                          refNode = node
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
              println("m_Joined " + numm_Joined)
                   refNode ! Print
        }

      }
      case _ => 
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
    /*
    val loop:Int = 4 
    //println(loop)
    var res: Long = 0
    for(i<-0 to loop-1)
       res = (res << 4 ) | Character.digit(sha.charAt(i), 16);
    res = res >> 2
    res = res & 0xFFFC /*Fetching first 14 bits in resultant string*/
    //println(res)
    return res
    */
    
    val loop:Int = 1 
    //println(loop)
    var res: Long = 0
    for(i<-0 to loop-1)
       res = (res << 4 ) | Character.digit(sha.charAt(i), 16);
    
    res = res & 0xF /*Fetching first 14 bits in resultant string*/
  //  res = res >> 1
    //println(res)
    return res
    

   }

 }

class Peer(nodeID : Long) extends Actor{
  var fingerTable = new Array[FingerProp](Global.max_len) 
     var Successor:ActorRef = self
     var Predecessor:ActorRef = self
     var refNode : ActorRef=null
     var m_maxnodes : Long = Global.m_maxnodes
     val m : Int = Global.max_len
 def fingertableinit ():Unit={
  for(i <-0 until m) {
    val start=(My_NodeID()+(math.pow(2, i)).toLong) % m_maxnodes
    val end=(My_NodeID()+(math.pow(2, i+1) ).toLong)% m_maxnodes
    val interval= new Interval(true, start,end, false)
    fingerTable(i)= new FingerProp(start,interval,self,nodeID)
  }
}
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
      if(interval.inValid(fingerTable(i).get_NodeID())) {
        return fingerTable(i).node;
      }
    }
    return self;
  }


  def init_finger_table():Unit = {
    fingerTable(0).setNode(Successor)
    fingerTable(0).Set_NodeID(My_NodeID(Successor))
      //  println("Node    " +self+"init_finger_table Entry at 0"+Successor)

    for(i<-0 until m-1){
      val interval=new Interval(true,My_NodeID(),fingerTable(i).get_NodeID(),true)
      if(interval.inValid(fingerTable(i+1).getStart())) {
           //println("Node    " +self +"init_finger_table Entry at "+i+"is" +fingerTable(i).getNode())
        fingerTable(i+1).setNode(fingerTable(i).getNode())
        fingerTable(i+1).Set_NodeID(My_NodeID(fingerTable(i).getNode()))
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


  override def receive: Receive ={

    case m_Join(asknode:ActorRef)=>{
      println("m_Join")
      this.refNode=asknode
      refNode!m_locateposition(self,My_NodeID)
      sender ! "m_Joined"
    }

    case m_nodePosLocated(predecessor:ActorRef,successor:ActorRef)=>{
      this.Predecessor=predecessor
      this.Successor=successor
      Predecessor!m_setSuccessor(self)
      Successor!m_setPredecessor(self)
      println("Located")
      println(" self "+My_NodeID(self)+" Successor "+My_NodeID(Successor)+ " Predecessor "+ My_NodeID(Predecessor))
      init_finger_table()
      //println("Finger Initiated")

      update_others()
     // println("Others Updated")
      sender ! "m_Joined"
    }

    case Found_Finger_Entry(i:Int,successor:ActorRef)=>{
      println("Node    " +My_NodeID(self) +"Found_Finger_Entry Entry at "+i+ "is "+My_NodeID(successor))
      this.fingerTable(i).setNode(successor)
      this.fingerTable(i).Set_NodeID(My_NodeID(successor))
    }

    case m_locateposition(node:ActorRef,nodeHash:Long)=>{
      val interval = new Interval(false, My_NodeID(), fingerTable(0).get_NodeID(), true)
      if(interval.inValid(nodeHash)){
        node!m_nodePosLocated(self,this.Successor)
      }else{

        val target=closest_preceding_finger(nodeHash)
        target!m_locateposition(node,nodeHash)
      }
    }

    case Find_Finger_Entry(node:ActorRef,i:Int,start:Long)=>{
      val interval = new Interval(false, My_NodeID(), fingerTable(0).get_NodeID(), true)
      if(interval.inValid(start)){
        node!Found_Finger_Entry(i,Successor)
      }else{
        val target=closest_preceding_finger(start)
        target!Find_Finger_Entry(node,i,start)
      }
    }
    case Update_Finger_Entry(before:Long,i:Int,node:ActorRef,nodeHash:Long)=>{
      if(node!=self) {
        val interval1 = new Interval(false, My_NodeID(), fingerTable(0).get_NodeID(), true)
        if (interval1.inValid(before)) { //I am the node just before N-2^i
            val interval2=new Interval(false, My_NodeID(), fingerTable(i).get_NodeID(), false)
            if(interval2.inValid(nodeHash)){
            println("Node    " +My_NodeID(self) +"Update_Finger_Entry Entry at "+i+ " is "+My_NodeID(node))
              fingerTable(i).setNode(node)
              fingerTable(i).Set_NodeID(My_NodeID(node))
              Predecessor!Update_Finger_Entry(My_NodeID(),i,node,nodeHash)
            }
        }else{
          val target=closest_preceding_finger(before)
          target!Update_Finger_Entry(before,i,node,nodeHash)
        }
      }
    }

    case m_setPredecessor(node:ActorRef)=>{
      this.Predecessor=node
    }

    case m_setSuccessor(node:ActorRef)=>{
      this.Successor=node
    }

    case Print =>{
      println("============================================")
      println("Node: %s".format(self.toString()))
      println("Hash: %s".format(My_NodeID()))
      println("Predecessor: %d".format(My_NodeID(Predecessor)))
      println("Successor: %d".format(My_NodeID(Successor)))
      println("Finger Table: ")
      for(i<- 0 until m) {
        println("   %d : ".format(i)+fingerTable(i).print)
      }
      println("============================================")
     
    }
    Thread.sleep(1000)
    Successor ! Print
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
  def Set_NodeID(newnodeid :Long):Unit={
     this.nodeID = newnodeid
  }
  def get_NodeID():Long={
    return this.nodeID
  }
  def setNode(newNode:ActorRef):Unit ={
    this.node=newNode
  }

  def print:String={
    return ("Start: %s, End: %s, Node: %d".format(start,interval.getEnd,get_NodeID()))
  }
}

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

