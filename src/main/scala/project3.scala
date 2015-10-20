import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import java.security.MessageDigest
import scala.util.Random
import akka.actor.PoisonPill
import scala.collection.mutable.HashMap


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
     var nodemap = new HashMap[Int, ActorRef]
     val m_maxnodes = 1024
     }


class Peer(nodeId:Int)extends Actor{
     var fingertable = new Array[ActorRef](7) //2 ^ m  m =7
     var Successor:Int = -1
     var Predecessor:Int = -1
     def receive = {
      case "Firstjoin" =>{
          println("First Node Joined \n")
          sender ! "joined"
        }
      case "join" =>{}
      case _=>
     }
     def iniatialize()={
    //  Successor = -1
    //  Predecessor = -1

      for(i<-0 to 7)
      {
       fingertable(i) = null 
      }      
     }
    def getSuccessor(nodeId:Int):Int={
     // Successor = 0

      return 0

     }
     def getPredecessor(nodeId:Int):Int={
     // Predecessor = 0
      return 0

     }
  }

/************************************************************************************/
  class ChordNetwork(numofnodes:Int , numRequest: Int ) extends Actor {

    
    val system = ActorSystem("Peer")
   
   // val starttime = system.currentTimeMillis()
    var numjoined:Int = 0
   
    var nodeid : Int = 0
    var node:ActorRef = null
     def receive = {
      case createNetwork=>{
          println("Network create initiating \n")
          for( i<-0 to numofnodes){
            nodeid = consistenthash(i)                   
             node = system.actorOf(Props(new Peer(nodeid)),"Peer")
            Global.nodemap.put(nodeid,node)

            if(i == 0)
               node ! "Firstjoin"
            else
               node ! "join"   
               } 
             }
      case joined => {
           numjoined = numofnodes + 1
           if(numjoined == numofnodes){
                 //for (peer <- Global.nodemap.ActorRef) {
          var msg: String = "hello"
         // peer ! startRouting(msg)
       // }
        }
      }

      //case "routingfinished"=>{}
 
      case _ => 
    }
   def consistenthash(index:Int): Int={
    var index1:String = "1"
    var sha:String = ""
    sha = MessageDigest.getInstance("SHA-1").digest(index1.getBytes("UTF-8")).map("%02x".format(_)).mkString
   // sha = sha.toInt
    //sha = sha & (0xFE<<18)
    //println(sha)
   // return sha  
   return 0
 }
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
