package it.unitn.ds1;
import java.io.IOException;

import akka.actor.Actor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;

public class Main {

  final static int N = 2; // degree of replication
  final static int R = 2; // read quorum
  final static int W = 2; // write quorum
  final static int T = 5; // timeout

  public static void main(String[] args) {
    if(R + W <= N || W <= N/2){
      System.out.println("N, R e W non sono stati settati correttamente per sequential consistency");
      return;
    }

    // Create the actor system
    final ActorSystem system = ActorSystem.create("ds1-project");

    // 1. Create node group

    //// create node n1
    ActorRef n1 = system.actorOf(Node.props(N, R, W, T),"n1");

    //// send init system to n1
    n1.tell(new Message.InitSystem(20), ActorRef.noSender());

    try { Thread.sleep(2 * T * 1000); }
    catch (InterruptedException e) { e.printStackTrace(); }

    n1.tell(new Message.PrintNodeList(), ActorRef.noSender());  // ask to print the current list of peers

    //// create node n2
    ActorRef n2 = system.actorOf(Node.props(N, R, W, T),"n2");

    //// send to n2 the message to allow it joininig the network
    n2.tell(new Message.JoinMsg(30, n1), ActorRef.noSender());

    try { Thread.sleep(2 * T * 1000); }
    catch (InterruptedException e) { e.printStackTrace(); }

    n2.tell(new Message.PrintNodeList(), ActorRef.noSender());  // ask to print the current list of peers

    ///// node n2 crashes
    n2.tell(new Message.CrashMsg(), ActorRef.noSender());

    try { Thread.sleep(2 * T * 1000); }
    catch (InterruptedException e) { e.printStackTrace(); }

    //// create node n3
    ActorRef n3 = system.actorOf(Node.props(N, R, W, T),"n3");

    //// send to n3 the message to allow it joining the network
    n3.tell(new Message.JoinMsg(40, n1), ActorRef.noSender());

    try { Thread.sleep(2 * T * 1000); }
    catch (InterruptedException e) { e.printStackTrace(); }

    n3.tell(new Message.PrintNodeList(), ActorRef.noSender());  // ask to print the current list of peers

    n1.tell(new Message.PrintNodeList(), ActorRef.noSender());  // ask to print the current list of peers

    n2.tell(new Message.PrintNodeList(), ActorRef.noSender());  // ask to print the current list of peers

    //// node n2 recovery
    n2.tell(new Message.RecoveryMsg(n1), Actor.noSender());

    try { Thread.sleep(2 * T * 1000); }
    catch (InterruptedException e) { e.printStackTrace(); }

    n2.tell(new Message.PrintNodeList(), ActorRef.noSender());  // ask to print the current list of peers

    //...end step 1

    // 2. Create client nodes and perform read and write operations
    ActorRef c1 = system.actorOf(Client.props(),"c1");
    ActorRef c2 = system.actorOf(Client.props(),"c2");

    // perform write operations
    c1.tell(new ClientMessage.Update(new Item(6, "VALUE6"), n1), ActorRef.noSender());
    c1.tell(new ClientMessage.Update(new Item(7, "VALUE7"), n2), ActorRef.noSender());
    c1.tell(new ClientMessage.Update(new Item(8, "VALUE8"), n3), ActorRef.noSender());

    try { Thread.sleep(2 * T * 1000); }
    catch (InterruptedException e) { e.printStackTrace(); }

    c1.tell(new ClientMessage.Update(new Item(15, "VALUE15"), n1), ActorRef.noSender());

    try { Thread.sleep(2 * T * 1000); }
    catch (InterruptedException e) { e.printStackTrace(); }

    c1.tell(new ClientMessage.Update(new Item(60, "VALUE60"), n2), ActorRef.noSender());
    c2.tell(new ClientMessage.Update(new Item(25, "VALUE25"), n3), ActorRef.noSender());
    c2.tell(new ClientMessage.Update(new Item(28, "VALUE28"), n1), ActorRef.noSender());

    try { Thread.sleep(2 * T * 1000); }
    catch (InterruptedException e) { e.printStackTrace(); }

    c2.tell(new ClientMessage.Update(new Item(33, "VALUE33"), n2), ActorRef.noSender());
    c1.tell(new ClientMessage.Update(new Item(49, "VALUE49"), n3), ActorRef.noSender());

    try { Thread.sleep(2 * T * 1000); }
    catch (InterruptedException e) { e.printStackTrace(); }

    // perform read operations
    c1.tell(new ClientMessage.Get(new Item(7, ""),n1), ActorRef.noSender());
    c1.tell(new ClientMessage.Get(new Item(15, ""),n1), ActorRef.noSender());
    c1.tell(new ClientMessage.Get(new Item(33, ""),n1), ActorRef.noSender());

    try { Thread.sleep(2 * T * 1000); }
    catch (InterruptedException e) { e.printStackTrace(); }

    System.out.println("LOCK TEST 1");

    // update item
    c1.tell(new ClientMessage.Update(new Item(15, "VALUE15_updated"), n1), ActorRef.noSender());
    c2.tell(new ClientMessage.Update(new Item(15, "CONFLICTING_15"), n3), ActorRef.noSender());

    try { Thread.sleep(2 * T * 1000); }
    catch (InterruptedException e) { e.printStackTrace(); }

    // read updated item
    c2.tell(new ClientMessage.Get(new Item(15, ""), n1), ActorRef.noSender());

    try { Thread.sleep(2 * T * 1000); }
    catch (InterruptedException e) { e.printStackTrace(); }

    System.out.println("LOCK TEST 2");

    // update item
    c2.tell(new ClientMessage.Update(new Item(15, "CONFLICTING_15"), n3), ActorRef.noSender());
    c1.tell(new ClientMessage.Update(new Item(15, "LAST VERSION"), n2), ActorRef.noSender());

    try { Thread.sleep(2 * T * 1000); }
    catch (InterruptedException e) { e.printStackTrace(); }

    // read updated item
    c2.tell(new ClientMessage.Get(new Item(15, ""), n1), ActorRef.noSender());
    c1.tell(new ClientMessage.Get(new Item(49, ""), n3), ActorRef.noSender());
    //...end step 2

    // 3. Join
    try { Thread.sleep(2 * T * 1000); }
    catch (InterruptedException e) { e.printStackTrace(); }

    ActorRef n4 = system.actorOf(Node.props(N, R, W, T),"n4");
    ActorRef n5 = system.actorOf(Node.props(N, R, W, T),"n5");

    n4.tell(new Message.JoinMsg(10, n1), ActorRef.noSender());

    try { Thread.sleep(2 * T * 1000); }
    catch (InterruptedException e) { e.printStackTrace(); }

    n5.tell(new Message.JoinMsg(50, n3), ActorRef.noSender());    
    // ...end join

    try { Thread.sleep(2 * T * 1000); }
    catch (InterruptedException e) { e.printStackTrace(); }

    System.out.println("LOCK TEST 3");

    c2.tell(new ClientMessage.Update(new Item(49, "CONFLICTING_49"), n1), ActorRef.noSender());
    c1.tell(new ClientMessage.Update(new Item(49, "LAST VERSION 49"), n2), ActorRef.noSender());

    try { Thread.sleep(2 * T * 1000); }
    catch (InterruptedException e) { e.printStackTrace(); }


    c2.tell(new ClientMessage.Get(new Item(49, ""), n3), ActorRef.noSender());

    try { Thread.sleep(2 * T * 1000); }
    catch (InterruptedException e) { e.printStackTrace(); }

    // print item set of the nodes
    
    try { Thread.sleep(2 * T * 1000); }
    catch (InterruptedException e) { e.printStackTrace(); }

    n1.tell(new Message.PrintItemList(), ActorRef.noSender());
    n2.tell(new Message.PrintItemList(), ActorRef.noSender());
    n3.tell(new Message.PrintItemList(), ActorRef.noSender());
    n4.tell(new Message.PrintItemList(), ActorRef.noSender());
    n5.tell(new Message.PrintItemList(), ActorRef.noSender());

    // ...end print item set of the nodes

    // leaving
    
    try { Thread.sleep(2 * T * 1000); }
    catch (InterruptedException e) { e.printStackTrace(); }

    //// leave n1
    n1.tell(new Message.LeaveMsg(), ActorRef.noSender());

    // print item set of the nodes
    try { Thread.sleep(2 * T * 1000); }
    catch (InterruptedException e) { e.printStackTrace(); }
    n2.tell(new Message.PrintItemList(), ActorRef.noSender());
    n3.tell(new Message.PrintItemList(), ActorRef.noSender());
    n4.tell(new Message.PrintItemList(), ActorRef.noSender());
    n5.tell(new Message.PrintItemList(), ActorRef.noSender());
    // ...end print item set of the nodes

    //// leave n2
    n2.tell(new Message.LeaveMsg(), ActorRef.noSender());

    // print item set of the nodes
    try { Thread.sleep(2 * T * 1000); }
    catch (InterruptedException e) { e.printStackTrace(); }
    n3.tell(new Message.PrintItemList(), ActorRef.noSender());
    n4.tell(new Message.PrintItemList(), ActorRef.noSender());
    n5.tell(new Message.PrintItemList(), ActorRef.noSender());
    // ...end print item set of the nodes

    //// leave n3
    n3.tell(new Message.LeaveMsg(), ActorRef.noSender());

    // print item set of the nodes
    try { Thread.sleep(2 * T * 1000); }
    catch (InterruptedException e) { e.printStackTrace(); }
    n4.tell(new Message.PrintItemList(), ActorRef.noSender());
    n5.tell(new Message.PrintItemList(), ActorRef.noSender());
    // ...end print item set of the nodes

    //// leave n4
    n4.tell(new Message.LeaveMsg(), ActorRef.noSender());

    // print item set of the nodes
    try { Thread.sleep(2 * T * 1000); }
    catch (InterruptedException e) { e.printStackTrace(); }
    n5.tell(new Message.PrintItemList(), ActorRef.noSender());
    // ...end print item set of the nodes

    // ... end leaving

    // test crash and recovery

    try { Thread.sleep(2000); }
    catch (InterruptedException e) { e.printStackTrace(); }

    //// create node n6
    ActorRef n6 = system.actorOf(Node.props(N, R, W, T),"n6");

    //// send to n6 the message to allow it joininig the network
    n6.tell(new Message.JoinMsg(40, n5), ActorRef.noSender());

    try { Thread.sleep(1000); }
    catch (InterruptedException e) { e.printStackTrace(); }

    n6.tell(new Message.PrintNodeList(), ActorRef.noSender());  // ask to print the current list of peers
    
    // print item set of the nodes
    try { Thread.sleep(1000); }
    catch (InterruptedException e) { e.printStackTrace(); }
    n5.tell(new Message.PrintItemList(), ActorRef.noSender());
    // ...end print item set of the nodes

    // print item set of the nodes
    try { Thread.sleep(1000); }
    catch (InterruptedException e) { e.printStackTrace(); }
    n6.tell(new Message.PrintItemList(), ActorRef.noSender());
    // ...end print item set of the nodes

    //// create node n7
    ActorRef n7 = system.actorOf(Node.props(N, R, W, T),"n7");

    //// send to n7 the message to allow it joininig the network
    n7.tell(new Message.JoinMsg(65, n5), ActorRef.noSender());

    try { Thread.sleep(1000); }
    catch (InterruptedException e) { e.printStackTrace(); }

    // print item set of the nodes
    try { Thread.sleep(1000); }
    catch (InterruptedException e) { e.printStackTrace(); }
    n5.tell(new Message.PrintItemList(), ActorRef.noSender());
    // ...end print item set of the nodes

    // print item set of the nodes
    try { Thread.sleep(1000); }
    catch (InterruptedException e) { e.printStackTrace(); }
    n7.tell(new Message.PrintItemList(), ActorRef.noSender());
    // ...end print item set of the nodes

    ///// node n6 crashes
    n6.tell(new Message.CrashMsg(), ActorRef.noSender());

    try { Thread.sleep(1000); }
    catch (InterruptedException e) { e.printStackTrace(); }

    // print item set of the nodes
    try { Thread.sleep(1000); }
    catch (InterruptedException e) { e.printStackTrace(); }
    n5.tell(new Message.PrintItemList(), ActorRef.noSender());
    // ...end print item set of the nodes

    // write new items
    c1.tell(new ClientMessage.Update(new Item(39, "VALUE39"), n5), ActorRef.noSender());
    c1.tell(new ClientMessage.Update(new Item(48, "VALUE48"), n5), ActorRef.noSender());
    c1.tell(new ClientMessage.Update(new Item(64, "VALUE48"), n5), ActorRef.noSender());

    //// node n6 recovery
    try { Thread.sleep(3000); }
    catch (InterruptedException e) { e.printStackTrace(); }
    n6.tell(new Message.RecoveryMsg(n5), Actor.noSender());

    // print item set of the nodes
    try { Thread.sleep(1000); }
    catch (InterruptedException e) { e.printStackTrace(); }
    n5.tell(new Message.PrintItemList(), ActorRef.noSender());
    // ...end print item set of the nodes

    // print item set of the nodes
    try { Thread.sleep(1000); }
    catch (InterruptedException e) { e.printStackTrace(); }
    n6.tell(new Message.PrintItemList(), ActorRef.noSender());
    // ...end print item set of the nodes

    // print item set of the nodes
    try { Thread.sleep(1000); }
    catch (InterruptedException e) { e.printStackTrace(); }
    n7.tell(new Message.PrintItemList(), ActorRef.noSender());
    // ...end print item set of the nodes

    // ... end test crash and recovery

    // test timeout reqActiveNodeList

    ///// node n7 crashes
    n7.tell(new Message.CrashMsg(), ActorRef.noSender());

    try { Thread.sleep(3000); }
    catch (InterruptedException e) { e.printStackTrace(); }

    ///// join n8
    ActorRef n8 = system.actorOf(Node.props(N, R, W, T),"n8");
    n8.tell(new Message.JoinMsg(64, n7), ActorRef.noSender());

    try { Thread.sleep(10000); }
    catch (InterruptedException e) { e.printStackTrace(); }

    ///// node n7 recover
    n7.tell(new Message.RecoveryMsg(n5), ActorRef.noSender());
    
    try { Thread.sleep(2000); }
    catch (InterruptedException e) { e.printStackTrace(); }

    //// n8 print current list of peers
    n8.tell(new Message.PrintNodeList(), ActorRef.noSender());  // ask to print the current list of peers
    // print item set of the nodes
    try { Thread.sleep(1000); }
    catch (InterruptedException e) { e.printStackTrace(); }
    n8.tell(new Message.PrintItemList(), ActorRef.noSender());
    // ...end print item set of the nodes

    // ... end test timeout reqActiveNodeList

    // test timeout reqDataItemsResponsibleFor
    
    ///// node n7 crashes
    n7.tell(new Message.CrashMsg(), ActorRef.noSender());

    try { Thread.sleep(3000); }
    catch (InterruptedException e) { e.printStackTrace(); }

    ///// join n8
    n8.tell(new Message.JoinMsg(63, n5), ActorRef.noSender());

    try { Thread.sleep(10000); }
    catch (InterruptedException e) { e.printStackTrace(); }

    ///// node n7 recover
    n7.tell(new Message.RecoveryMsg(n5), ActorRef.noSender());
    
    try { Thread.sleep(2000); }
    catch (InterruptedException e) { e.printStackTrace(); }

    //// n8 print current list of peers
    n8.tell(new Message.PrintNodeList(), ActorRef.noSender());  // ask to print the current list of peers
    // print item set of the nodes
    try { Thread.sleep(1000); }
    catch (InterruptedException e) { e.printStackTrace(); }
    n8.tell(new Message.PrintItemList(), ActorRef.noSender());
    // ...end print item set of the nodes

    // ... end test timeout reqDataItemsResponsibleFor

    // test timeout AnnounceDeparture
    ///// node n6 crashes
    n6.tell(new Message.CrashMsg(), ActorRef.noSender());

    try { Thread.sleep(2000); }
    catch (InterruptedException e) { e.printStackTrace(); }

    //// node n7 leave
    n7.tell(new Message.LeaveMsg(), ActorRef.noSender());

    try { Thread.sleep(10000); }
    catch (InterruptedException e) { e.printStackTrace(); }

    ///// node n6 recover
    n6.tell(new Message.RecoveryMsg(n5), ActorRef.noSender());

    //// n7 print current list of peers
    n7.tell(new Message.PrintNodeList(), ActorRef.noSender());  // ask to print the current list of peers
    // print item set of the nodes
    try { Thread.sleep(1000); }
    catch (InterruptedException e) { e.printStackTrace(); }
    n7.tell(new Message.PrintItemList(), ActorRef.noSender());
    // ...end print item set of the nodes

    // ... end test timeout AnnounceDeparture

    // test timeout JoinReadOperationReq
    System.out.println("");
    System.out.println("################################### test timeout JoinReadOperationReq");
    System.out.println("");

    try { Thread.sleep(3000); }
    catch (InterruptedException e) { e.printStackTrace(); }

    //// n5 print current list of peers
    n5.tell(new Message.PrintNodeList(), ActorRef.noSender());  // ask to print the current list of peers
    // print item set of the nodes
    try { Thread.sleep(1000); }
    catch (InterruptedException e) { e.printStackTrace(); }
    n5.tell(new Message.PrintItemList(), ActorRef.noSender());

    try { Thread.sleep(2000); }
    catch (InterruptedException e) { e.printStackTrace(); }

    //// node n5 crash
    n5.tell(new Message.CrashMsg(), ActorRef.noSender());

    try { Thread.sleep(3000); }
    catch (InterruptedException e) { e.printStackTrace(); }

    ///// join n8
    n8.tell(new Message.JoinMsg(63, n6), ActorRef.noSender());

    try { Thread.sleep(10000); }
    catch (InterruptedException e) { e.printStackTrace(); }

    //// node n5 recover
    n5.tell(new Message.RecoveryMsg(n6), ActorRef.noSender());

    //// n8 print current list of peers
    n8.tell(new Message.PrintNodeList(), ActorRef.noSender());  // ask to print the current list of peers
    // print item set of the nodes
    try { Thread.sleep(1000); }
    catch (InterruptedException e) { e.printStackTrace(); }
    n8.tell(new Message.PrintItemList(), ActorRef.noSender());
    // ...end print item set of the nodes

    // ... end test timeout JoinReadOperationReq

    // test timeout ReqActiveNodeList_recover
    System.out.println("");
    System.out.println("################################### test timeout ReqActiveNodeList_recover");
    System.out.println("");

    try { Thread.sleep(3000); }
    catch (InterruptedException e) { e.printStackTrace(); }

    //// node n5 crash
    n5.tell(new Message.CrashMsg(), ActorRef.noSender());

    try { Thread.sleep(3000); }
    catch (InterruptedException e) { e.printStackTrace(); }

    //// node n6 crash
    n6.tell(new Message.CrashMsg(), ActorRef.noSender());

    try { Thread.sleep(2000); }
    catch (InterruptedException e) { e.printStackTrace(); }

    //// node n6 recover
    n6.tell(new Message.RecoveryMsg(n5), ActorRef.noSender());

    try { Thread.sleep(10000); }
    catch (InterruptedException e) { e.printStackTrace(); }

    //// node n5 recover
    n5.tell(new Message.RecoveryMsg(n7), ActorRef.noSender());

    try { Thread.sleep(2000); }
    catch (InterruptedException e) { e.printStackTrace(); }

    //// node n6 recover
    n6.tell(new Message.RecoveryMsg(n5), ActorRef.noSender());

    try { Thread.sleep(2000); }
    catch (InterruptedException e) { e.printStackTrace(); }

    //// n6 print current list of peers
    n6.tell(new Message.PrintNodeList(), ActorRef.noSender());  // ask to print the current list of peers
    // print item set of the nodes
    try { Thread.sleep(1000); }
    catch (InterruptedException e) { e.printStackTrace(); }
    n5.tell(new Message.PrintItemList(), ActorRef.noSender());
    // ...end print item set of the nodes

    // ... end test timeout ReqActiveNodeList_recover

    // test timeout ReqDataItemsResponsibleFor_recovery
    System.out.println("");
    System.out.println("################################### test timeout ReqDataItemsResponsibleFor_recovery");
    System.out.println("");

    try { Thread.sleep(3000); }
    catch (InterruptedException e) { e.printStackTrace(); }

    //// node n5 crash
    n5.tell(new Message.CrashMsg(), ActorRef.noSender());

    try { Thread.sleep(3000); }
    catch (InterruptedException e) { e.printStackTrace(); }

    //// node n7 crash
    n7.tell(new Message.CrashMsg(), ActorRef.noSender());

    try { Thread.sleep(3000); }
    catch (InterruptedException e) { e.printStackTrace(); }

    //// node n5 recover
    n5.tell(new Message.RecoveryMsg(n6), ActorRef.noSender());

    try { Thread.sleep(10000); }
    catch (InterruptedException e) { e.printStackTrace(); }

    //// node n7 recover
    n7.tell(new Message.RecoveryMsg(n6), ActorRef.noSender());
    
    try { Thread.sleep(3000); }
    catch (InterruptedException e) { e.printStackTrace(); }

    //// node n5 recover
    n5.tell(new Message.RecoveryMsg(n6), ActorRef.noSender());

    // ... end test timeout ReqDataItemsResponsibleFor_recovery

    try {
      System.out.println(">>> Press ENTER to exit <<<");
      System.in.read();
    }
    catch (IOException ioe) {}
    finally {
      system.terminate();
    }
  }
}
