import akka.actor.{Actor, ActorRef, ActorSystem, Props, PoisonPill}
import scala.collection.immutable.{TreeMap, List}
import java.security.MessageDigest
import scala.util.Random
import scala.collection.mutable.HashMap
import akka.util.Timeout
import java.math.BigInteger
import scala.collection.immutable.{TreeMap, List}


case class Firstjoin(nodeid : BigInt)
case class Join(asknode:ActorRef)
case class locateNode(node:ActorRef,nodeHash:BigInt)
case class nodeLocated(predecessor:ActorRef,successor:ActorRef)
case class Find_Finger(node:ActorRef,i:Int,start:BigInt)
case class Found_Finger(i:Int,successor:ActorRef)
case class Update_Finger(before:BigInt,i:Int,node:ActorRef,nodeHash:BigInt)
case class Find(node:ActorRef,code:String, step:Int)
case class Found(code: String,predecessor:ActorRef,successor:ActorRef,step:Int)
case class setPredecessor(node:ActorRef)
case class setSuccessor(node:ActorRef)
case object Print
//case class Joined()



object Global {
    var nodemap = new HashMap[BigInt, ActorRef]
     val max_len =4
     val m_maxnodes:BigInt = BigInt(2).pow(max_len)
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
   // val starttime = system.currentTimeMillis()
    var numjoined:Int = 0
    var nodeID : BigInt = 0
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
       var nodeset = scala.collection.mutable.Set[BigInt]()
                  while(nodeset.size != numofnodes)
                  {
                     nodeset += consistenthash(Random.nextInt(2000000))
                     //println(nodeset.size)
                   }
                  for( i<-0 to numofnodes-1){  
                     Thread.sleep(100)          
                     try { 
                            nodeID = consistenthash(Random.nextInt(2000000))
                            //nodeID = nodeset.toVector(i)
                            node = (system1.actorOf(Props(new Peer(nodeID)),name = getmyname(nodeID))) 
                                         // ...
                      } catch {
                        case e: Exception => {
                          if(retrycount < 10){
                            println("Actor name clash tring max try again")
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
                         node ! Join(refNode)  
                            Thread.sleep(100) 
                         } 
                    Global.nodemap.put(nodeID,node)
                  }
                   // println(" i"+i)
                    /*for( i<-0 to numofnodes-1){ 
                     
                       var nodeID = nodeset.toVector(i) 
                    //   node =  Global.nodemap.get(nodeID)
                    node =Global.nodemap(nodeID)
                        if(i==0)
                        {
                          refNode = node
                        }
                        else
                        {    
                         node ! Join(refNode)  
                            Thread.sleep(100) 
                         } 
                      }*/

       }
      
      case "Joined" => {

           numjoined = numjoined + 1
            if(numjoined == numofnodes - 1){
              println("joined " + numjoined)
                   refNode ! Print
        }

      }
      case _ => 
    }
   def consistenthash(index:Int): BigInt={
    var index1:String = index.toString
    var sha:String = ""
    sha = MessageDigest.getInstance("SHA-1").digest(index1.getBytes("UTF-8")).map("%02x".format(_)).mkString

   val res:BigInt = Parsefirstmbits(sha)
   return res
 }
   def getmyname (nodeId: BigInt):String ={
    return nodeId.toString
   }
   def Parsefirstmbits(sha:String):BigInt={
    /*
    val loop:Int = 4 
    //println(loop)
    var res: BigInt = 0
    for(i<-0 to loop-1)
       res = (res << 4 ) | Character.digit(sha.charAt(i), 16);
    res = res >> 2
    res = res & 0xFFFC /*Fetching first 14 bits in resultant string*/
    //println(res)
    return res
    */
    
    val loop:Int = 1 
    //println(loop)
    var res: BigInt = 0
    for(i<-0 to loop-1)
       res = (res << 4 ) | Character.digit(sha.charAt(i), 16);
    
    res = res & 0xF /*Fetching first 14 bits in resultant string*/
  //  res = res >> 1
    //println(res)
    return res
    

   }

 }

class Peer(nodeID : BigInt) extends Actor{
  var fingerTable = new Array[FingerProp](Global.max_len) //2 ^ m  m =7
     var Successor:ActorRef = self
     var Predecessor:ActorRef = self
     var refNode : ActorRef=null
     var m_maxnodes : BigInt = Global.m_maxnodes
     val m : Int = Global.max_len
  for(i <-0 until m) {
    val start=(My_NodeID()+BigInt(2).pow(i))%(BigInt(2).pow(m))
    val end=(My_NodeID()+BigInt(2).pow(i+1))%(BigInt(2).pow(m))
    val interval= new Interval(true, start,end, false)
    fingerTable(i)= new FingerProp(start,interval,self,nodeID)
  }
  def My_NodeID():BigInt={
          val pattern = "([0-9]+)".r
          val pattern(num) = self.path.name
          //println(num.toInt)
          return num.toInt
  }
  def My_NodeID(node:ActorRef):BigInt={
          val pattern = "([0-9]+)".r
          val pattern(num) = node.path.name
          //println(num.toInt)
          return num.toInt
  }


  def closest_preceding_finger(id:BigInt): ActorRef = {
    val interval=new Interval(false,My_NodeID(),id,false)
    for(i <- m-1 to 0 by -1){
      if(interval.inValid(fingerTable(i).get_NodeID())) {
        return fingerTable(i).node;
      }
    }
    return self;
  }


  def init_fingers():Unit = {
    fingerTable(0).setNode(Successor)
    fingerTable(0).Set_NodeID(My_NodeID(Successor))
      //  println("Node    " +self+"init_fingers Entry at 0"+Successor)

    for(i<-0 until m-1){
      val interval=new Interval(true,My_NodeID(),fingerTable(i).get_NodeID(),true)
      if(interval.inValid(fingerTable(i+1).getStart())) {
           //println("Node    " +self +"init_fingers Entry at "+i+"is" +fingerTable(i).getNode())
        fingerTable(i+1).setNode(fingerTable(i).getNode())
        fingerTable(i+1).Set_NodeID(My_NodeID(fingerTable(i).getNode()))
      }
      else{
        if(refNode!=null){
          refNode!Find_Finger(self,i+1,fingerTable(i+1).getStart())
        }
      }
    }
  }

  def update_others():Unit = {
    for(i <- 0 to m-1) {
      val position=(My_NodeID()-BigInt(2).pow(i)+BigInt(2).pow(m)+1)%BigInt(2).pow(m)
      Successor!Update_Finger(position,i,self,My_NodeID())
    }
  }


  override def receive: Receive ={

    case Join(asknode:ActorRef)=>{
      println("Join")
      this.refNode=asknode
      refNode!locateNode(self,My_NodeID)
      sender ! "Joined"
    }

    case nodeLocated(predecessor:ActorRef,successor:ActorRef)=>{
      this.Predecessor=predecessor
      this.Successor=successor
      Predecessor!setSuccessor(self)
      Successor!setPredecessor(self)
      println("Located")
      println(" self "+My_NodeID(self)+" Successor "+My_NodeID(Successor)+ " Predecessor "+ My_NodeID(Predecessor))
      init_fingers()
      //println("Finger Initiated")

      update_others()
     // println("Others Updated")
      sender ! "Joined"
    }

    case Found_Finger(i:Int,successor:ActorRef)=>{
      println("Node    " +My_NodeID(self) +"Found_Finger Entry at "+i+ "is "+My_NodeID(successor))
      this.fingerTable(i).setNode(successor)
      this.fingerTable(i).Set_NodeID(My_NodeID(successor))
    }

    case locateNode(node:ActorRef,nodeHash:BigInt)=>{
      val interval = new Interval(false, My_NodeID(), fingerTable(0).get_NodeID(), true)
      if(interval.inValid(nodeHash)){
        node!nodeLocated(self,this.Successor)
      }else{

        val target=closest_preceding_finger(nodeHash)
        target!locateNode(node,nodeHash)
      }
    }

    case Find_Finger(node:ActorRef,i:Int,start:BigInt)=>{
      val interval = new Interval(false, My_NodeID(), fingerTable(0).get_NodeID(), true)
      if(interval.inValid(start)){
        node!Found_Finger(i,Successor)
      }else{
        val target=closest_preceding_finger(start)
        target!Find_Finger(node,i,start)
      }
    }
    case Update_Finger(before:BigInt,i:Int,node:ActorRef,nodeHash:BigInt)=>{
      if(node!=self) {
        val interval1 = new Interval(false, My_NodeID(), fingerTable(0).get_NodeID(), true)
        if (interval1.inValid(before)) { //I am the node just before N-2^i
            val interval2=new Interval(false, My_NodeID(), fingerTable(i).get_NodeID(), false)
            if(interval2.inValid(nodeHash)){
            println("Node    " +My_NodeID(self) +"Update_Finger Entry at "+i+ " is "+My_NodeID(node))
              fingerTable(i).setNode(node)
              fingerTable(i).Set_NodeID(My_NodeID(node))
              Predecessor!Update_Finger(My_NodeID(),i,node,nodeHash)
            }
        }else{
          val target=closest_preceding_finger(before)
          target!Update_Finger(before,i,node,nodeHash)
        }
      }
    }

    case setPredecessor(node:ActorRef)=>{
      this.Predecessor=node
    }

    case setSuccessor(node:ActorRef)=>{
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



class FingerProp(start: BigInt, interval: Interval, var node:ActorRef,var nodeID:BigInt){
  def getStart(): BigInt = {
    return this.start
  }
  def getInterval(): Interval = {
    return this.interval
  }
  def getNode(): ActorRef = {
    return this.node
  }
  def Set_NodeID(newnodeid :BigInt):Unit={
     this.nodeID = newnodeid
  }
  def get_NodeID():BigInt={
    return this.nodeID
  }
  def setNode(newNode:ActorRef):Unit ={
   // println("Entry Added"+newNode)
    this.node=newNode
  }

  def print:String={
    return ("Start: %s, End: %s, Node: %d".format(start,interval.getEnd,get_NodeID()))
  }


}; 

/* Working Properly 
object Main extends App{
  val system=ActorSystem("system")
  val node0=system.actorOf(Props(new Peer()), name = "0")

  //Thread.sleep(1000)
  val node2=system.actorOf(Props(new Peer()), name = "3")
  val node1=system.actorOf(Props(new Peer()), name = "2")
  val node4=system.actorOf(Props(new Peer()), name = "7")

  //Thread.sleep(1000)

  val node3=system.actorOf(Props(new Peer()), name = "6")


  Thread.sleep(100)
  node1!Join(node0)
  Thread.sleep(100)
  node3!Join(node0)
  Thread.sleep(100)
  node2!Join(node0)
  Thread.sleep(100)
  node4!Join(node0)



  Thread.sleep(1000)

  node0!Print
  Thread.sleep(100)
  node1!Print
  Thread.sleep(100)
  node2!Print
  Thread.sleep(100)
  node3!Print
  Thread.sleep(100)
  node4!Print

  Thread.sleep(1000)

  node4!Find(node4,"55555555555555555555555555555555",0)

  Thread.sleep(1000)

  system.shutdown()
}
*/

class Interval(includeStart:Boolean,start:BigInt,end:BigInt, includeEnd:Boolean){
  def inValid(nodetobefound:BigInt): Boolean = { 
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

  def getEnd(): BigInt ={
    return end
  }
}

