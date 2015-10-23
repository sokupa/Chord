
package Chord
import akka.actor.{Actor, ActorRef, ActorSystem, Props}
//import ExecutionContext.Implicits.global
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
case class Find_Finger(node:ActorRef,i:Int,start:BigInt)
case class FingerEntry_Found(i:Int,successor:ActorRef)
case class Find_FingerEntry(node:ActorRef,i:Int,start:BigInt)
//case class FingerEntry_Found(i:Int,successor:ActorRef)

  object Global {
     var nodemap = new HashMap[BigInt, ActorRef]
     val max_len =14
     val m_maxnodes:BigInt = BigInt(2).pow(max_len)
     }

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

class Interval(includeStart:Boolean,start:BigInt,end:BigInt, includeEnd:Boolean){
  def isValid(nodetobefound:BigInt): Boolean = { 
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
}

class FingerProp(start: BigInt, interval: Interval, var node:ActorRef, var nodeid: BigInt){

  def setNode(updatednode:ActorRef) ={
     this.node=updatednode
  }
  def setnodeid(updatednodeid:BigInt) ={
     this.nodeid=updatednodeid
  }
  def getNode(): ActorRef = {
    return this.node
  }
  def getnodeid():BigInt ={
    return this.nodeid
  }
  def getStart(): BigInt = {
    return this.start
  }
  def getInterval(): Interval = {
    return this.interval
  } 
 
}
 


/************************************************************************************/
  class ChordNetwork(numofnodes:Int , numRequest: Int ) extends Actor {

    
    val system = ActorSystem("Peer")
   
   // val starttime = system.currentTimeMillis()
    var numjoined:Int = 0
   
    var nodeid : BigInt = 0
    var node:ActorRef = null
    var nodesMap = new HashMap[String,ActorRef]
    var nodeidList = new Array[String](numofnodes)
    var sortedNodeList = List.empty[String]
    var totalHopCount = 0.0f
    var aggregate = 0.0f
    var averageHopCount = 0.0f 
    var retrycount:Int = 0   
    var refnode:ActorRef = null
    def receive = {
      case "createNetwork"=>{
          println("Network create initiating \n")
          var nodeset = scala.collection.mutable.Set[BigInt]()
          while(nodeset.size != numofnodes)
          {
             nodeset += consistenthash(Random.nextInt(2000000))
             //println(nodeset.size)
           }
          for( i<-0 to numofnodes-1){            
             try { 
                   // nodeid = consistenthash(Random.nextInt(2000000))
                    nodeid = nodeset.toVector(i)
                    node = (system.actorOf(Props(new Peer(nodeid)),name = getmyname(nodeid))) 
                                 // ...
              } catch {
                case e: Exception => {
                  if(retrycount < 10){
                    println("Actor name clash tring max try again")
                  nodeid = consistenthash(Random.nextInt(2000000))
                    node = (system.actorOf(Props(new Peer(nodeid)),name = getmyname(nodeid))) 
                    retrycount = retrycount +1
                  }
              }                   
             }
            Global.nodemap.put(nodeid,node)
            
            if(i == 1)
            {
               refnode = node
               node ! Firstjoin(nodeid)
            }
            else{
              refnode = actor.Selection(""+Random.nextInt(nodeset.size))
              println(nodeset.toVector(Random.nextInt(nodeset.size)))
             //  node ! Join(refnode,nodeid)  

             }
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
      case _ => 
    }
   def consistenthash(index:Int): BigInt={
    var index1:String = index.toString
    var sha:String = ""
    sha = MessageDigest.getInstance("SHA-1").digest(index1.getBytes("UTF-8")).map("%02x".format(_)).mkString

   val res:BigInt = Parsefirstmbits(sha)
   return res
 }
   def getmyname (nodeid: BigInt):String ={
    return nodeid.toString
   }
   def Parsefirstmbits(sha:String):BigInt={
    val loop:Int = 4 
    //println(loop)
    var res: BigInt = 0
    for(i<-0 to loop-1)
       res = (res << 4 ) | Character.digit(sha.charAt(i), 16);
    res = res >> 2
    res = res & 0xFFFC /*Fetching first 14 bits in resultant string*/
    //println(res)
    return res
   }

 }



class Finger (nodeid:BigInt,node:ActorRef,Twomult :Int){
    private  var   m_start:BigInt = 0
    private  var   m_end:BigInt = 0
    private  var   m_range: BigInt = 0
    private  var   m_nodeid:BigInt = 0
    private  var   m_node :ActorRef = null
    def my_nodeid():BigInt={
      return m_nodeid
    }
    def my_Actor():ActorRef={
      return m_node
    }
    def changenode(toChange: ActorRef):Unit ={
      m_node = toChange
    }
    def getstart(): BigInt={
      return m_start
    }
    def getend():BigInt ={
      return m_end
    }
    def getrange ():BigInt ={
      return m_range
    }
    def init_finger():Unit={
      m_start = nodeid + math.pow(2,Twomult).toInt
      m_end =  nodeid + math.pow(2,Twomult+1).toInt
      m_range = m_end - m_start
      m_node = node
      m_nodeid = nodeid
    }
    def doCotain(nodeval:BigInt):Boolean={
      if(nodeval >= m_start && nodeval <=m_end)
      return true
      else
      return false
    }
}


class Peer(nodeID:BigInt)extends Actor{
     var fingerTable = new Array[FingerProp](14) //2 ^ m  m =7
     var Successor:ActorRef = null
     var Predecessor:ActorRef = null
     var refnode : ActorRef=null
     var nodeid : BigInt = 0
     var m_maxnodes : BigInt = Global.m_maxnodes
     var m : Int = Global.max_len
     def getnodeid(node:ActorRef):BigInt={
          val pattern = "([0-9]+)".r
          val pattern(num) = node.path.name
          println(num.toInt)
          return num.toInt
     }

     def receive = {
      case Firstjoin(nodeid : BigInt) =>{
          println("First Node Joined \n")
          this.nodeid = nodeid
          initializeFingerTable(nodeid)
          sender ! joined()
        }
      case Join(asknode:ActorRef,nodeid : BigInt) =>{
        this.nodeid = nodeid
        this.refnode = asknode
        println(nodeid)
        initializeFingerTable(nodeid)
        asknode!locateNode(self,nodeid)
      
        update_self()
        stablize()
        sender ! joined()
      }

      case locateNode(nodetobefound:ActorRef,nodeidtobefound:BigInt)=>{
        val interval = new Interval(false,nodeid,fingerTable(0).nodeid, true)
        if(interval.isValid(nodeidtobefound)) {
          nodetobefound ! nodeLocated(self,this.Successor)
        }
        else{
        var nextnode : ActorRef = closest_preceding_successor(nodeid,nodeidtobefound)
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

      case Find_FingerEntry(node:ActorRef,i:Int,start:BigInt)=>{
        val interval = new Interval(false, nodeid, fingerTable(0).getnodeid, true)
        if(interval.isValid(fingerTable(i+1).getStart)){
          node!FingerEntry_Found(i,Successor)
         }else{
          var nextnode : ActorRef=closest_preceding_successor(nodeid,start)
          nextnode!Find_FingerEntry(node,i,start)
        }
      }
      case FingerEntry_Found(i:Int,successor:ActorRef)=>{
        this.fingerTable(i).setNode(successor)
      }
      case _=>
      }

     def update_self():Unit={
          fingerTable(0).setNode(Successor)
         // fingerTable(0).setnodeid(successor's node id)   
       for(i<-0 until Global.max_len-1){
        /*
          val interval=new Interval(true,nodeid,fingerTable(i).getnodeid(),true)
          if(interval.isValid(fingerTable(i+1).getStart())) {
            fingerTable(i+1).setNode(fingerTable(i).getNode())
          }

          else{
          */
              refnode!Find_FingerEntry(self,i+1,fingerTable(i+1).getStart())
           // }
          }
        }

     def stablize():Unit={
        for(i <- 0 to m-1) {
         // val position=(getHash()-BigInt(2).pow(i)+BigInt(2).pow(m)+1)%BigInt(2).pow(m)
         // successor!Update_Finger_Entries(position,i,self,getHash())
        }
     }
    def closest_preceding_successor(currnodeid:BigInt,nodeidtobefound:BigInt) : ActorRef = {  
        val interval=new Interval(false,currnodeid,nodeidtobefound,false)
        for(i <- m-1 to 0 by -1){
          if(interval.isValid(fingerTable(i).getnodeid)) {
            return fingerTable(i).getNode();
          }
      }
      //return self;  Disagree
       return fingerTable(m-1).node;// To be checked after confirmiing bhaviour of Interval
    }
    def initializeFingerTable(nodeid : BigInt) = { // Updating Interval Left
        for(i <-0 until Global.max_len-1) { // Some implementation say it should be m but I disagree
          val start = nodeid + BigInt(2).pow(i)%(m_maxnodes)
          val end = nodeid + BigInt(2).pow(i+1)%(m_maxnodes)
          val interval= new Interval(true, start ,end, false)
          fingerTable(i)= new FingerProp(start,interval,self,nodeid)
        }

    }

}

