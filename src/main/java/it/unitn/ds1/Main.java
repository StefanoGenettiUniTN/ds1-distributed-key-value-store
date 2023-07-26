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
    ActorRef n1 = system.actorOf(Node.props(20, N, R, W, T),"n1");

    //// send init system to n1
    n1.tell(new Message.InitSystem(), ActorRef.noSender());

    try { Thread.sleep(1000); }
    catch (InterruptedException e) { e.printStackTrace(); }

    n1.tell(new Message.PrintNodeList(), ActorRef.noSender());  // ask to print the current list of peers

    //// create node n2
    ActorRef n2 = system.actorOf(Node.props(30, N, R, W, T),"n2");

    //// send to n2 the message to allow it joininig the network
    n2.tell(new Message.JoinMsg(30, n1), ActorRef.noSender());

    try { Thread.sleep(1000); }
    catch (InterruptedException e) { e.printStackTrace(); }

    n2.tell(new Message.PrintNodeList(), ActorRef.noSender());  // ask to print the current list of peers

    ///// node n2 crashes
    n2.tell(new Message.CrashMsg(), ActorRef.noSender());

    try { Thread.sleep(1000); }
    catch (InterruptedException e) { e.printStackTrace(); }

    //// create node n3
    ActorRef n3 = system.actorOf(Node.props(40, N, R, W, T),"n3");

    //// send to n3 the message to allow it joininig the network
    n3.tell(new Message.JoinMsg(40, n1), ActorRef.noSender());

    try { Thread.sleep(1000); }
    catch (InterruptedException e) { e.printStackTrace(); }

    n3.tell(new Message.PrintNodeList(), ActorRef.noSender());  // ask to print the current list of peers

    n1.tell(new Message.PrintNodeList(), ActorRef.noSender());  // ask to print the current list of peers

    n2.tell(new Message.PrintNodeList(), ActorRef.noSender());  // ask to print the current list of peers

    //// node n2 recovery
    n2.tell(new Message.RecoveryMsg(n1), Actor.noSender());

    try { Thread.sleep(1000); }
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
    c1.tell(new ClientMessage.Update(new Item(15, "VALUE15"), n1), ActorRef.noSender());
    c1.tell(new ClientMessage.Update(new Item(60, "VALUE60"), n2), ActorRef.noSender());
    c2.tell(new ClientMessage.Update(new Item(25, "VALUE25"), n3), ActorRef.noSender());
    c2.tell(new ClientMessage.Update(new Item(28, "VALUE28"), n1), ActorRef.noSender());
    c2.tell(new ClientMessage.Update(new Item(33, "VALUE33"), n2), ActorRef.noSender());
    c2.tell(new ClientMessage.Update(new Item(49, "VALUE49"), n3), ActorRef.noSender());

    try { Thread.sleep(1000); }
    catch (InterruptedException e) { e.printStackTrace(); }

    // perform read operations
    c1.tell(new ClientMessage.Get(new Item(7, ""),n1), ActorRef.noSender());
    c1.tell(new ClientMessage.Get(new Item(15, ""),n1), ActorRef.noSender());
    c1.tell(new ClientMessage.Get(new Item(33, ""),n1), ActorRef.noSender());

    try { Thread.sleep(1000); }
    catch (InterruptedException e) { e.printStackTrace(); }

    // update item
    c1.tell(new ClientMessage.Update(new Item(15, "VALUE15_updated"), n1), ActorRef.noSender());
    c1.tell(new ClientMessage.Update(new Item(15, "CONFLICTING_15"), n3), ActorRef.noSender());

    try { Thread.sleep(1000); }
    catch (InterruptedException e) { e.printStackTrace(); }

    // read updated item
    c2.tell(new ClientMessage.Get(new Item(15, ""), n1), ActorRef.noSender());

    try { Thread.sleep(1000); }
    catch (InterruptedException e) { e.printStackTrace(); }

    // update item
    c1.tell(new ClientMessage.Update(new Item(15, "CONFLICTING_15"), n3), ActorRef.noSender());
    c1.tell(new ClientMessage.Update(new Item(15, "LAST VERSION"), n2), ActorRef.noSender());

    try { Thread.sleep(1000); }
    catch (InterruptedException e) { e.printStackTrace(); }

    // read updated item
    c2.tell(new ClientMessage.Get(new Item(15, ""), n1), ActorRef.noSender());
    //...end step 2

    // 3. Join
    try { Thread.sleep(1000); }
    catch (InterruptedException e) { e.printStackTrace(); }

    ActorRef n4 = system.actorOf(Node.props(10, N, R, W, T),"n4");
    ActorRef n5 = system.actorOf(Node.props(50, N, R, W, T),"n5");

    n4.tell(new Message.JoinMsg(10, n1), ActorRef.noSender());

    try { Thread.sleep(1000); }
    catch (InterruptedException e) { e.printStackTrace(); }

    n5.tell(new Message.JoinMsg(50, n3), ActorRef.noSender());    
    // ...end join

    // print item set of the nodes
    
    try { Thread.sleep(1000); }
    catch (InterruptedException e) { e.printStackTrace(); }

    n1.tell(new Message.PrintItemList(), ActorRef.noSender());
    n2.tell(new Message.PrintItemList(), ActorRef.noSender());
    n3.tell(new Message.PrintItemList(), ActorRef.noSender());
    n4.tell(new Message.PrintItemList(), ActorRef.noSender());
    n5.tell(new Message.PrintItemList(), ActorRef.noSender());

    // ...end print item set of the nodes

    // leaving
    
    try { Thread.sleep(2000); }
    catch (InterruptedException e) { e.printStackTrace(); }

    //// leave n1
    n1.tell(new Message.LeaveMsg(), ActorRef.noSender());

    // print item set of the nodes
    try { Thread.sleep(1000); }
    catch (InterruptedException e) { e.printStackTrace(); }
    n2.tell(new Message.PrintItemList(), ActorRef.noSender());
    n3.tell(new Message.PrintItemList(), ActorRef.noSender());
    n4.tell(new Message.PrintItemList(), ActorRef.noSender());
    n5.tell(new Message.PrintItemList(), ActorRef.noSender());
    // ...end print item set of the nodes

    //// leave n2
    n2.tell(new Message.LeaveMsg(), ActorRef.noSender());

    // print item set of the nodes
    try { Thread.sleep(1000); }
    catch (InterruptedException e) { e.printStackTrace(); }
    n3.tell(new Message.PrintItemList(), ActorRef.noSender());
    n4.tell(new Message.PrintItemList(), ActorRef.noSender());
    n5.tell(new Message.PrintItemList(), ActorRef.noSender());
    // ...end print item set of the nodes

    //// leave n3
    n3.tell(new Message.LeaveMsg(), ActorRef.noSender());

    // print item set of the nodes
    try { Thread.sleep(1000); }
    catch (InterruptedException e) { e.printStackTrace(); }
    n4.tell(new Message.PrintItemList(), ActorRef.noSender());
    n5.tell(new Message.PrintItemList(), ActorRef.noSender());
    // ...end print item set of the nodes

    //// leave n4
    n4.tell(new Message.LeaveMsg(), ActorRef.noSender());

    // print item set of the nodes
    try { Thread.sleep(1000); }
    catch (InterruptedException e) { e.printStackTrace(); }
    n5.tell(new Message.PrintItemList(), ActorRef.noSender());
    // ...end print item set of the nodes

    // ... end leaving

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
