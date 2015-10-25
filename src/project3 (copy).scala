import akka.actor.{Actor, ActorRef, ActorSystem, Props, PoisonPill}
import scala.collection.immutable.{TreeMap, List}
import scala.concurrent.{Await, Future, Promise}
// import scala.concurrent.Future
 import scala.concurrent.duration._
//import ExecutionContext.Implicits.global
import java.security.MessageDigest
import scala.util.Random
import scala.collection.mutable.HashMap
import akka.util.Timeout
import java.math.BigInteger
import akka.pattern.ask 
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
case class FingerUpdate(toupdate: ActorRef, tofind:BigInt,fingerindex: Int)


  object Global {
     var nodemap = new HashMap[BigInt, ActorRef]
     val max_len =4
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

class FingerProp(start: BigInt, interval: Interval,end : BigInt, var node:ActorRef, var nodeid: BigInt){
 // private  count = 0
  
  def setNode(updatednode:ActorRef) ={
     this.node = updatednode
     //println("nodeid-----| "+nodeid+/*" start "+start+" end "+end+*/" updatednode -----|"+updatednode)
  }
  def setNodeID(updatednodeid:BigInt) ={
     this.nodeid = updatednodeid
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
   def getEnd(): BigInt = {
    return this.end
  }
  def getInterval(): Interval = {
    return this.interval
  } 
 
}
 

class Peer(nodeId:BigInt)extends Actor{
     var fingerTable = new Array[FingerProp](14) //2 ^ m  m =7
     var Successor:ActorRef = self
     var Predecessor:ActorRef = self
     var refnode : ActorRef=null
     var nodeID : BigInt = 0
     var m_maxnodes : BigInt = Global.m_maxnodes
     var m : Int = Global.max_len
     initializeFingerTable(nodeId)
     def getnodeid(node:ActorRef):BigInt={
          val pattern = "([0-9]+)".r
          val pattern(num) = node.path.name
          //println(num.toInt)
          return num.toInt
     }

     def receive = {
      case Firstjoin(nodeid : BigInt) =>{
          println("First Node Joined \n")
          this.nodeID = nodeid
         // println("First node"+nodeid)
          Successor = self
          Predecessor =self
         // initializeFingerTable(nodeID)
          sender ! joined()
        }
      case Join(asknode:ActorRef,nodeid : BigInt) =>{
        this.nodeID = nodeId
        this.refnode = asknode
        
        //println("asknode"+asknode)
       // implicit val timeout = Timeout(5.seconds)
      //  val result = asknode ? locateNode(self,nodeid)
       asknode ! locateNode(self,nodeid)
       sender ! joined()
        //println("result "+result)     
     //   Thread.sleep(5000)          
      }
      case "printfinger"=>{
        println("**********node********* "+getnodeid(self)+" my successor "+getnodeid(Successor))
        for(i<-0 until m)
          println("Entry at pos "+i+" is "+fingerTable(i).getNodeID())
        Successor ! "printfinger"
        Thread.sleep(1000) 
      }

      case locateNode(nodetobefound:ActorRef,nodeidtobefound:BigInt)=>{
        val interval = new Interval(false,nodeID,fingerTable(0).nodeid, true)
        if(interval.isValid(nodeidtobefound)) {
                //    println("locateNode inside if*************")
                  //  println("locateNode self "+getnodeid(self)+" successor "+getnodeid(Successor)+" predecessor "+getnodeid(Predecessor))
          nodetobefound ! nodeLocated(self,this.Successor)
        }
        else{
        var nextnode : ActorRef = closest_preceding_successor(nodeID,nodeidtobefound)
         // println("locateNode inside else*************")
         // println("locateNode self "+getnodeid(self)+" successor "+getnodeid(Successor)+" predecessor "+getnodeid(Predecessor)+" nextnode "+nextnode)
          nextnode!locateNode(nodetobefound,nodeidtobefound)
       }
     }

      case nodeLocated(predecessor:ActorRef,successor:ActorRef)=>{
          Predecessor=predecessor
          Successor=successor
          //println(" nodeLocated Predecessor "+getnodeid(Predecessor)+" Successor "+getnodeid(Successor)+" self "+getnodeid(self))
          Predecessor!set_Successor(self) // update my predecessor successor
          Successor!set_Predecessor(self) // update my successor predecessor
          //initializeFingerTable(nodeID)
          update_self()
          stablize()
        
        }

      case set_Successor (succ:ActorRef)=>
         // println(" set_Successor succ "+getnodeid(succ)+" nodeself "+getnodeid(self))
          Successor = succ
      case set_Predecessor (pred:ActorRef)=>
       //   println("set_Predecessor"+pred)
         // println(" set_Predecessor succ "+getnodeid(pred)+" nodeself "+getnodeid(self))
          Predecessor = pred

      case Find_FingerEntry(node:ActorRef,i:Int,start:BigInt)=>{
        val interval = new Interval(false, nodeID, fingerTable(0).getNodeID, true)
        if(interval.isValid(start)){
          println("Find_FingerEntry self---"+getnodeid(self)+" setnodeid "+getnodeid(Successor))
          this.fingerTable(i).setNode(Successor)
          this.fingerTable(i).setNodeID(getnodeid(Successor))
          //node!FingerEntry_Found(i,Successor)
         }else{
          var nextnode : ActorRef=closest_preceding_successor(nodeID,start)
          nextnode!Find_FingerEntry(node,i,start)
        }
      }
      case FingerEntry_Found(i:Int,successor:ActorRef)=>{
        println("--------------FingerEntry_Found--------------self---"+getnodeid(self)+" setnodeid "+getnodeid(successor))
        this.fingerTable(i).setNode(successor)
        this.fingerTable(i).setNodeID(getnodeid(successor))
      }
     case FingerUpdate(newnode: ActorRef, tofind : BigInt, i:Int)=>{
        //var updatepred : Int = 0 
       // println("FingerUpdate newnode outside "+getnodeid(newnode)+" self "+getnodeid(self))
        if(newnode != self)
        {
          val Interval = new Interval(false, getnodeid(self),fingerTable(0).getNodeID(),true)
          if(Interval.isValid(tofind)){
            val nextinterval = new Interval (false, getnodeid(self),fingerTable(i).getNodeID(),false)
             if(nextinterval.isValid(getnodeid(newnode))){
              //println("FingerUpdate i "+i+" newnode "+getnodeid(newnode))
              fingerTable(i).setNode(newnode)
              fingerTable(i).setNodeID(getnodeid(newnode))
              Predecessor ! FingerUpdate(newnode,getnodeid(self),i)
             }
           }
             else{
              val next = closest_preceding_successor(getnodeid(newnode),tofind)
              //println("FingerUpdate else newnode "+getnodeid(next)+" tofind "+tofind+" self "+getnodeid(self))
              next ! FingerUpdate(newnode , tofind ,i)
            
             }          
        }
        /*for(i<-0 until m )
        {
          println("**********FIngertable********")
          print(fingerTable(i).getNodeID()+" ")
        }*/
        /*{
          for(i<- 0 until Global.max_len)
          {
              val interval = new Interval(true,fingerTable(i).getStart,fingerTable(i).getEnd,true)

              if(interval.isValid(newnodeID))
              {
               // updatepred = 1
                println("--------------FingerUpdate-------------self---"+getnodeid(self)+" setnodeid "+getnodeid(newnode))
                fingerTable(i).setNode(newnode) // Set Node ID is left to Actor Selection. To be done after merging th changes.
              }
          }
        //  if(updatepred == 1)
            Predecessor ! FingerUpdate(newnode,newnodeID)
        }*/
      
      }

     /* case FingerUpdate(toupdate: ActorRef, fingerindex: Int)=>{
          var diff = (getnodeid(toupdate) - getnodeid(self))%BigInt(2).pow(1)
          if(diff < 0)
          {
           diff = BigInt(2).pow(m) - getnodeid(self)+getnodeid(toupdate) - 0

           println(diff)
         }
           //diff = diff % 2
        var loop:Double = Math.log(diff.doubleValue())
         println (loop)
         var myval  = loop.toInt
         Thread.sleep(100)  
         // var loop:Int = diff.toInt
          if(myval < m)
          {
          for(i<-0 to myval)
          {
            val check = new Interval(true,getnodeid(self),fingerTable(i).getNodeID(),false)
            //val offset = fingerTable(i).getNodeID - getnodeid(toupdate)
            if(check.isValid(getnodeid(toupdate))){
              fingerTable(i).setNode(toupdate)
              fingerTable(i).setNodeID(getnodeid(toupdate))
            }
          }
         Predecessor ! FingerUpdate(toupdate, fingerindex)
       }
      }*/

      case _=>
      }

     def update_self():Unit={
          //println("update_self self "+getnodeid(self)+" successor "+getnodeid(Successor)+" predecessor "+getnodeid(Predecessor))
          fingerTable(0).setNode(Successor)
          fingerTable(0).setNodeID(getnodeid(Successor))
       for(i<-0 until Global.max_len-1){
          val interval=new Interval(true,nodeID,fingerTable(i).getNodeID(),true)
          if(interval.isValid(fingerTable(i+1).getStart())) {
                //println(" update_self self "+getnodeid(self)+" setnode "+getnodeid(fingerTable(i).getNode())+ " i "+i)
            fingerTable(i+1).setNode(fingerTable(i).getNode())
            fingerTable(i+1).setNodeID(fingerTable(i).getNodeID())
          }
          else{
              refnode!Find_FingerEntry(self,i+1,fingerTable(i+1).getStart())
          }
        }
      }
     def stablize():Unit={
      for(i<- 0 until m){
     //   println("Inside stablize")
        var jumpto = nodeID - BigInt(2).pow(i)+BigInt(2).pow(m)+1
        jumpto = jumpto % BigInt(2).pow(m)
        Successor ! FingerUpdate(self,jumpto,i)
       // Predecessor ! FingerUpdate(self,i)

      }

     }
   /* def immediate_successor():ActorRef{

    }*/
    def closest_preceding_successor(currnodeID:BigInt,nodeidtobefound:BigInt) : ActorRef = {  
        val interval=new Interval(false,currnodeID,nodeidtobefound,false)
        for(i <- m-1 to 0 by -1){
          if(interval.isValid(fingerTable(i).getNodeID)) {
            return fingerTable(i).getNode();
          }
      }
      return self;  
       //return fingerTable(m-1).node;// To be checked after confirmiing bhaviour of Interval
    }

   /* def find_predecessor(m_checker: ActorRef,tofind :BigInt):ActorRef={
      var checker = m_checker
      println(" find_predecessor m_checker-before while-"+getnodeid(self)+" Successor "+getnodeid(Successor)+" tofind "+tofind+" m_checker "+getnodeid(m_checker))
      while((tofind > getnodeid(checker) && tofind<=getnodeid(Successor)) == false){
         println(" find_predecessor predecessor--inside while 11 "+getnodeid(checker)+" Successor "+getnodeid(Successor)+" tofind "+tofind)   
         checker = closest_preceding_successor(getnodeid(checker),tofind)
         println(" find_predecessor predecessor--inside while 22 "+getnodeid(checker)+" Successor "+getnodeid(Successor)+" tofind "+tofind)    
       }
       println(" find_predecessor predecessor-after while-"+getnodeid(checker)+" Successor "+getnodeid(Successor)+" tofind "+tofind)   
      return checker
    }*/
    def initializeFingerTable(nodeID : BigInt) = {
        for(i <-0 until Global.max_len) { 
          val start = (nodeID + BigInt(2).pow(i))%(m_maxnodes)
          val end = (nodeID + BigInt(2).pow(i+1))%(m_maxnodes)
          val interval= new Interval(true, start ,end, false)
          fingerTable(i)= new FingerProp(start,interval,end,self,nodeID)
          //println("initializeFingerTable "+i )
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
    var retrycount:Int = 0   
    var refnode : ActorRef = null
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
                    nodeID = nodeset.toVector(i)
                    node = (system.actorOf(Props(new Peer(nodeID)),name = getmyname(nodeID))) 
                                 // ...
              } catch {
                case e: Exception => {
                  if(retrycount < 10){
                    println("Actor name clash tring max try again")
                  nodeID = consistenthash(Random.nextInt(2000000))
                    node = (system.actorOf(Props(new Peer(nodeID)),name = getmyname(nodeID))) 
                    retrycount = retrycount +1
                  }
              }                   
             }
            Global.nodemap.put(nodeID,node)
          }
           // println(" i"+i)
        for( i<-0 to numofnodes-1){
            var myid = nodeset.toVector(i)
            var node = Global.nodemap(myid)
            println(" node "+node+" nodeid "+myid)
            Thread.sleep(1000) 
            if(i == 0)
            {
               refnode = node;
               node ! Firstjoin(nodeID)
            }
            else
               node ! Join(refnode,nodeID)  
               
             }
           }
      case joined() => {
           numjoined = numjoined + 1
           println("joined numjoined "+numjoined)
           if(numjoined == numofnodes){
            node ! "printfinger"
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
   def getmyname (nodeId: BigInt):String ={
    return nodeId.toString
   }
   def Parsefirstmbits(sha:String):BigInt={
    /*val loop:Int = 4 
    //println(loop)
    var res: BigInt = 0
    for(i<-0 to loop-1)
       res = (res << 4 ) | Character.digit(sha.charAt(i), 16);
    res = res >> 2
    res = res & 0xFFFC /*Fetching first 14 bits in resultant string*/
    //println(res)
    return res*/
    val loop:Int = 1 
    //println(loop)
    var res: BigInt = 0
    for(i<-0 to loop-1)
       res = (res << 4 ) | Character.digit(sha.charAt(i), 16); 
    res = res & 0xF /*Fetching first 14 bits in resultant string*/
    //res = res >> 1
    //println(res)
    return res
   }

 }

