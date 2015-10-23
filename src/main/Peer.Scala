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

