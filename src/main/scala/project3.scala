import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import java.security.MessageDigest
import scala.util.Random
import akka.actor.PoisonPill
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashMap
import scala.collection.immutable.TreeMap
import scala.collection.immutable.List
import akka.util.Timeout
import java.math.BigInteger
case class set_Successor(succ: ActorRef)
case class set_Predecessor(pred:ActorRef)
case class fetch(succ:ActorRef , pred:ActorRef , curr :ActorRef)
case class joined()
case class Join(asknode:ActorRef,nodeid : BigInt)
case class Firstjoin(nodeid : BigInt)
case class locateNode(nodetobefound:ActorRef,nodeid:BigInt)
case class nodeLocated(predecessor:ActorRef,successor:ActorRef)

object start extends App{

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
  object Global {
     var nodemap = new HashMap[BigInt, ActorRef]
     val max_len =14
     val m_maxnodes:BigInt = BigInt(2).pow(max_len)
     }

/********************************************************************************************************************************************
***********************************************Class Peer**********************************************************************************
*Functions: *****************************Simulation of Chord join leave  ******************************************************
*join : returns neighbour of current node in line topology
*leave:returns neigbour for 3D and 3D imperfect topology check3dimp distinguishes both
*Bootstrap() : Retries after fixed instance of time
*getSuccessor():
*getPredecessor():
 *********************************************************************************************************************************************
*********************************************************************************************************************************************/

class Interval(leftInclude:Boolean,start:BigInt,end:BigInt, rightInclude:Boolean){
  def isValid(validity:BigInt): Boolean = { 



  return true
  } //TO DO
}

class FingerProp(start: BigInt, interval: Interval, var node:ActorRef, var nodeid: BigInt){

  def setNode(updatednode:ActorRef) ={
     this.node=updatednode
  }
  def setNodeID(updatednodeid:BigInt) ={
     this.nodeid=updatednodeid
  }
  def getNode(): ActorRef = {
    return this.node
  }
  def getNodeID():BigInt ={
    return this.nodeid
  }
  def getStart(): BigInt = {
    return this.start
  }
  /*
  def setStart(updatedstart:BigIn): BigInt = {
     this.start=updatedstart
  }
  */
  def getInterval(): Interval = {
    return this.interval
  } 
 
};
 

class Peer(nodeId:BigInt)extends Actor{
     var fingerTable = new Array[FingerProp](14) //2 ^ m  m =7
     var Successor:ActorRef = null
     var Predecessor:ActorRef = null
     var nodeID : BigInt = 0
     var m_maxnodes : BigInt = Global.m_maxnodes
     var m : Int = Global.max_len

     def receive = {
      case Firstjoin(nodeid : BigInt) =>{
          println("First Node Joined \n")
          this.nodeID = nodeid
          initializeFingerTable(nodeID)
          sender ! joined()
        }
      case Join(asknode:ActorRef,nodeid : BigInt) =>{
        this.nodeID = nodeId
        initializeFingerTable(nodeID)
        asknode!locateNode(self,nodeid)
        
        update_self()
        update_others()
        sender ! joined()
      }

      case locateNode(nodetobefound:ActorRef,nodeidtobefound:BigInt)=>{
        val interval = new Interval(false,nodeID,fingerTable(0).nodeid, true)
        if(interval.isValid(nodeidtobefound)) {
          nodetobefound ! nodeLocated(self,this.Successor)
        }
        else{
        var nextnode : ActorRef = closest_preceding_successor(nodeID,nodeidtobefound)
          nextnode!locateNode(nodetobefound,nodeidtobefound)
       }
     }

      case nodeLocated(predecessor:ActorRef,successor:ActorRef)=>{
          this.Predecessor=predecessor
          this.Successor=successor
          predecessor!set_Successor(self)//Add by myself
          successor!set_Predecessor(self)
        }

      case set_Successor (succ:ActorRef)=>
          Successor = succ
      case set_Predecessor (pred:ActorRef)=>
          Predecessor = pred

      case _=>
     }
     def initialize()={
      for(i<-0 to 7)
      {
      // fingertable(i) = null 
      }      
     }
     def update_self():Unit={
          println (self)
          sender ! fetch(Successor,Predecessor,self)
         for(i<-1 to Global.max_len){
          
         }
         
        // Predecessor ! set_Successor(self)
        // Successor ! set_Predecessor (self)
     }
     def update_others():Unit={

     }
     def chekfingerTable(nodeID: BigInt):Boolean={
        return true//To DO
     }
    def closest_preceding_successor(currnodeID:BigInt,nodeidtobefound:BigInt) : ActorRef = {  
        val interval=new Interval(false,currnodeID,nodeidtobefound,false)
        for(i <- m-1 to 0 by -1){
          if(interval.isValid(fingerTable(i).getNodeID)) {
            return fingerTable(i).getNode();
          }
      }
      //return self;  Disagree
       return fingerTable(m-1).node;// To be checked after confirmiing bhaviour of Interval
    }
    def initializeFingerTable(nodeID : BigInt) = { // Updating Interval Left
        for(i <-0 until m-1) { // Some implementation say it should be m but I disagree
          val start = nodeID + BigInt(2).pow(i)%(m_maxnodes)
          val end = nodeID + BigInt(2).pow(i+1)%(m_maxnodes)
          val interval= new Interval(true, start ,end, false)
          fingerTable(i)= new FingerProp(start,interval,self,nodeID)
        }

    }

}

/************************************************************************************/
  class ChordNetwork(numofnodes:Int , numRequest: Int ) extends Actor {

    
    val system = ActorSystem("Peer")
   
   // val starttime = system.currentTimeMillis()
    var numjoined:Int = 0
   
    var nodeID : BigInt = 0
    var node:ActorRef = null
    var nodesMap = new HashMap[String,ActorRef]
    var nodeIDList = new Array[String](numofnodes)
    var sortedNodeList = List.empty[String]
    var totalHopCount = 0.0f
    var aggregate = 0.0f
    var averageHopCount = 0.0f
    var refnode : ActorRef = null;
    def receive = {
      case "createNetwork"=>{
          println("Network create initiating \n")
          for( i<-1 to numofnodes){
            nodeID = consistenthash(i)                   
             node = system.actorOf(Props(new Peer(nodeID)))
            //Global.nodemap.put(nodeid,node)
            if(i>0)

            //println("\n i"+i)

            if(i == 1)
            {
               val refnode:ActorRef = node;
               node ! Firstjoin(nodeID)
            }
            else
               node ! Join(refnode,nodeID)  
               //i = i+1 
               } 
             }
      case joined() => {
           numjoined = numjoined + 1
           println("joined")
           if(numjoined == numofnodes){
                 //for (peer <- Global.nodemap.ActorRef) {
          var msg: String = "hello"
         // system.shutdown()
         // peer ! startRouting(msg)
       // }
        }
      }

     case fetch(successor :ActorRef , predecessor :ActorRef, from :ActorRef)=>{
      println("Inside fetch")

     }
 
      case _ => 
    }
   def consistenthash(index:Int): Int={
    var index1:String = index.toString
    var sha:String = ""
    sha = MessageDigest.getInstance("SHA-1").digest(index1.getBytes("UTF-8")).map("%02x".format(_)).mkString

    val res:Int = Parsefirstmbits(sha)
    return res
   }
   def Parsefirstmbits(sha:String):Int={
   // val minbits =  //math.floor(Global.m_maxnodes/16)
    val loop:Int = 4 //(minbits/4).toInt
   // var mask = Global.m_maxnodes & (Global.m_maxnodes -1)
    println(loop)
   // mask = generatemask(numofnodes)
    var res: Int = 0
    for(i<-0 to loop-1)
       res = (res << 4 ) | Character.digit(sha.charAt(i), 16);
    res = res >> 2
    res = res & 0xFFFC /*Fetching first 14 bits in resultant string*/
    println(res)
    return res

   }

 }


