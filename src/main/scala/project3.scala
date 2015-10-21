import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import java.security.MessageDigest
import scala.util.Random
import akka.actor.PoisonPill
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashMap
import scala.collection.immutable.TreeMap
import scala.collection.immutable.List
import akka.util.Timeout
case class set_Successor(succ: ActorRef)
case class set_Predecessor(pred:ActorRef)
case class fetch(succ:ActorRef , pred:ActorRef , curr :ActorRef)
case class joined()

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
     var nodemap = new HashMap[Long, ActorRef]
     val m_maxnodes = math.pow(2,14)
     val max_len =14
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


class Peer(nodeId:Long)extends Actor{
     var fingertable = new Array[ActorRef](14) //2 ^ m  m =7
     var Successor:ActorRef = null
     var Predecessor:ActorRef = null
     def receive = {
      case "Firstjoin" =>{
          println("First Node Joined \n")
          sender ! joined()
        }
      case "join" =>{
       
        update_self()
        update_others()
        sender ! joined()
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
       fingertable(i) = null 
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
  }

/************************************************************************************/
  class ChordNetwork(numofnodes:Int , numRequest: Int ) extends Actor {

    
    val system = ActorSystem("Peer")
   
   // val starttime = system.currentTimeMillis()
    var numjoined:Int = 0
   
    var nodeid : Long = 0
    var node:ActorRef = null
    var nodesMap = new HashMap[String,ActorRef]
    var nodeIdList = new Array[String](numberOfNodes)
    var sortedNodeList = List.empty[String]
    var totalHopCount = 0.0f
    var aggregate = 0.0f
    var averageHopCount = 0.0f
  
    def receive = {
      case "createNetwork"=>{
          println("Network create initiating \n")
          for( i<-1 to numofnodes){
            nodeid = consistenthash(i)                   
             node = system.actorOf(Props(new Peer(nodeid)))
            Global.nodemap.put(nodeid,node)
            //println("\n i"+i)

            if(i == 1)
               node ! "Firstjoin"
            else
               node ! "join"  
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
   def consistenthash(index:Int): Long={
    var index1:String = index.toString
    var sha:String = ""
    sha = MessageDigest.getInstance("SHA-1").digest(index1.getBytes("UTF-8")).map("%02x".format(_)).mkString

   val res:Long = Parsefirstmbits(sha)
   return res
 }
   def Parsefirstmbits(sha:String):Long={
   // val minbits =  //math.floor(Global.m_maxnodes/16)
    val loop:Int = 4 //(minbits/4).toInt
   // var mask = Global.m_maxnodes & (Global.m_maxnodes -1)
    println(loop)
   // mask = generatemask(numofnodes)
    var res: Long = 0
    for(i<-0 to loop-1)
       res = (res << 4 ) | Character.digit(sha.charAt(i), 16);
    res = res >> 2
    res = res & 0xFFFC /*Fetching first 14 bits in resultant string*/
    println(res)
    return res

   }

 }



