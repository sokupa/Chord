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
     var nodemap = new HashMap[Long, ActorRef]
     val m_maxnodes = math.pow(2,14)
     }


class Peer(nodeId:Long)extends Actor{
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
   
    var nodeid : Long = 0
    var node:ActorRef = null
    def receive = {
      case createNetwork=>{
          println("Network create initiating \n")
          for( i<-1 to numofnodes){
            nodeid = consistenthash(i)                   
             node = system.actorOf(Props(new Peer(nodeid)))
            Global.nodemap.put(nodeid,node)
            println("\n i"+i)

            if(i == 0)
               node ! "Firstjoin"
            else
               node ! "join"  
               //i = i+1 
               } 
             }
      case joined => {
           numjoined = numjoined + 1
           if(numjoined == numofnodes){
                 //for (peer <- Global.nodemap.ActorRef) {
          var msg: String = "hello"
          system.shutdown()
         // peer ! startRouting(msg)
       // }
        }
      }

      //case "routingfinished"=>{}
 
      case _ => 
    }
   def consistenthash(index:Int): Long={
    var index1:String = index.toString
    var sha:String = ""
    sha = MessageDigest.getInstance("SHA-1").digest(index1.getBytes("UTF-8")).map("%02x".format(_)).mkString
//val res:Array[Byte]  = sha.getBytes()
   val res:Long = Parsefirstmbits(sha)
    println(res)
    println(sha)
    //res:Int = Integer.parseInt(sha);
    //sha = sha.toInt
    //sha = sha & (0xFE<<18)
    //println(sha)
   // return sha  
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
