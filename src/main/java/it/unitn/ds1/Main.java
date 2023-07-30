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
  final static int SLEEPTIMESHORT = 3000;
  final static int SLEEPTIMEFULL = 2 * T * 1000;

  public static void main(String[] args) {
    if(R + W <= N || W <= N/2){
      System.out.println("N, R e W non sono stati settati correttamente per sequential consistency");
      return;
    }

    // Create the actor system
    final ActorSystem system = ActorSystem.create("ds1-project");

    // 1. Create node group
    System.out.println("========================================");
    System.out.println("Create Node n1(key:20) and join (first node in the ring); finally print the list of active node");
    //// create node n1
    ActorRef n1 = system.actorOf(Node.props(N, R, W, T),"n1");

    //// send init system to n1
    n1.tell(new Message.InitSystem(20), ActorRef.noSender());

    try { Thread.sleep(SLEEPTIMEFULL); }
    catch (InterruptedException e) { e.printStackTrace(); }

    n1.tell(new Message.PrintNodeList(), ActorRef.noSender());  // ask to print the current list of peers

    try { Thread.sleep(SLEEPTIMESHORT); }
    catch (InterruptedException e) { e.printStackTrace(); }

    System.out.println("========================================\n\n");

    System.out.println("========================================");
    System.out.println("Create Node n2(key:30) and join; finally it prints the list of active node");

    //// create node n2
    ActorRef n2 = system.actorOf(Node.props(N, R, W, T),"n2");

    //// send to n2 the message to allow it joininig the network
    n2.tell(new Message.JoinMsg(30, n1), ActorRef.noSender());

    try { Thread.sleep(SLEEPTIMEFULL); }
    catch (InterruptedException e) { e.printStackTrace(); }

    n2.tell(new Message.PrintNodeList(), ActorRef.noSender());  // ask to print the current list of peers

    try { Thread.sleep(SLEEPTIMESHORT); }
    catch (InterruptedException e) { e.printStackTrace(); }

    System.out.println("========================================\n\n");

    System.out.println("========================================");
    System.out.println("Node n2 crash");

    ///// node n2 crashes
    n2.tell(new Message.CrashMsg(), ActorRef.noSender());

    try { Thread.sleep(SLEEPTIMEFULL); }
    catch (InterruptedException e) { e.printStackTrace(); }

    try { Thread.sleep(SLEEPTIMESHORT); }
    catch (InterruptedException e) { e.printStackTrace(); }

    System.out.println("========================================\n\n");

    System.out.println("========================================");
    System.out.println("Create Node n3(key:40) and join; finally n1 and n3 print the list of active node, n2 no since it is crashed");

    //// create node n3
    ActorRef n3 = system.actorOf(Node.props(N, R, W, T),"n3");

    //// send to n3 the message to allow it joining the network
    n3.tell(new Message.JoinMsg(40, n1), ActorRef.noSender());

    try { Thread.sleep(SLEEPTIMEFULL); }
    catch (InterruptedException e) { e.printStackTrace(); }

    n3.tell(new Message.PrintNodeList(), ActorRef.noSender());  // ask to print the current list of peers

    n1.tell(new Message.PrintNodeList(), ActorRef.noSender());  // ask to print the current list of peers

    n2.tell(new Message.PrintNodeList(), ActorRef.noSender());  // ask to print the current list of peers

    try { Thread.sleep(SLEEPTIMESHORT); }
    catch (InterruptedException e) { e.printStackTrace(); }
    System.out.println("========================================\n\n");

    System.out.println("========================================");
    System.out.println("Try to insert a node with a key (40) already present");

    //// create node n3
    ActorRef n32 = system.actorOf(Node.props(N, R, W, T),"n32");

    //// send to n3 the message to allow it joining the network
    n32.tell(new Message.JoinMsg(40, n1), ActorRef.noSender());

    try { Thread.sleep(SLEEPTIMEFULL); }
    catch (InterruptedException e) { e.printStackTrace(); }

    n3.tell(new Message.PrintNodeList(), ActorRef.noSender());  // ask to print the current list of peers

    n1.tell(new Message.PrintNodeList(), ActorRef.noSender());  // ask to print the current list of peers

    n2.tell(new Message.PrintNodeList(), ActorRef.noSender());  // ask to print the current list of peers

    try { Thread.sleep(SLEEPTIMESHORT); }
    catch (InterruptedException e) { e.printStackTrace(); }
    System.out.println("========================================\n\n");

    System.out.println("========================================");
    System.out.println("N2 recovery and prints active node list");

    //// node n2 recovery
    n2.tell(new Message.RecoveryMsg(n1), Actor.noSender());

    try { Thread.sleep(2 * T * 1000); }
    catch (InterruptedException e) { e.printStackTrace(); }

    n2.tell(new Message.PrintNodeList(), ActorRef.noSender());  // ask to print the current list of peers

    try { Thread.sleep(SLEEPTIMESHORT); }
    catch (InterruptedException e) { e.printStackTrace(); }
    System.out.println("========================================\n\n");

   //...end step 1

    System.out.println("========================================");
    System.out.println("Create three clients");

    // 2. Create client nodes and perform read and write operations
    ActorRef c1 = system.actorOf(Client.props(),"c1");
    ActorRef c2 = system.actorOf(Client.props(),"c2");
    ActorRef c3 = system.actorOf(Client.props(),"c3");

    try { Thread.sleep(SLEEPTIMESHORT); }
    catch (InterruptedException e) { e.printStackTrace(); }
    System.out.println("========================================\n\n");

    System.out.println("========================================");
    System.out.println("Insert first items");

    // perform write operations
    c1.tell(new ClientMessage.Update(new Item(6, "VALUE6"), n1), ActorRef.noSender());
    c2.tell(new ClientMessage.Update(new Item(7, "VALUE7"), n2), ActorRef.noSender());
    c3.tell(new ClientMessage.Update(new Item(28, "VALUE28"), n3), ActorRef.noSender());

    try { Thread.sleep(SLEEPTIMEFULL); }
    catch (InterruptedException e) { e.printStackTrace(); }

    System.out.println("========================================\n\n");

    System.out.println("========================================");
    System.out.println("Insert other items");

    c1.tell(new ClientMessage.Update(new Item(15, "VALUE15"), n1), ActorRef.noSender());
    c2.tell(new ClientMessage.Update(new Item(60, "VALUE60"), n2), ActorRef.noSender());
    c3.tell(new ClientMessage.Update(new Item(25, "VALUE25"), n3), ActorRef.noSender());

    try { Thread.sleep(SLEEPTIMEFULL); }
    catch (InterruptedException e) { e.printStackTrace(); }

    System.out.println("========================================\n\n");

    System.out.println("========================================");
    System.out.println("Try to insert two insert other items from the same element, one of the two should fail");

    c1.tell(new ClientMessage.Update(new Item(33, "VALUE33"), n2), ActorRef.noSender());
    c1.tell(new ClientMessage.Update(new Item(49, "VALUE49"), n3), ActorRef.noSender());

    try { Thread.sleep(SLEEPTIMEFULL); }
    catch (InterruptedException e) { e.printStackTrace(); }

    System.out.println("========================================\n\n");

    System.out.println("========================================");
    System.out.println("Print list of items stored in each node");

    n1.tell(new Message.PrintItemList(), ActorRef.noSender());
    n2.tell(new Message.PrintItemList(), ActorRef.noSender());
    n3.tell(new Message.PrintItemList(), ActorRef.noSender());

    try { Thread.sleep(SLEEPTIMESHORT); }
    catch (InterruptedException e) { e.printStackTrace(); }

    System.out.println("========================================\n\n");

    System.out.println("========================================");
    System.out.println("Read items 7, 15 and 33");


    // perform read operations
    c1.tell(new ClientMessage.Get(new Item(7, ""),n1), ActorRef.noSender());
    c2.tell(new ClientMessage.Get(new Item(15, ""),n1), ActorRef.noSender());
    c3.tell(new ClientMessage.Get(new Item(33, ""),n3), ActorRef.noSender());

    try { Thread.sleep(SLEEPTIMEFULL); }
    catch (InterruptedException e) { e.printStackTrace(); }

    System.out.println("========================================\n\n");

    System.out.println("========================================");
    System.out.println("Read an unexisting item, a timeout is expected");


    // perform read operations
    c1.tell(new ClientMessage.Get(new Item(100, ""),n1), ActorRef.noSender());

    try { Thread.sleep(SLEEPTIMEFULL); }
    catch (InterruptedException e) { e.printStackTrace(); }

    System.out.println("========================================\n\n");

    System.out.println("========================================");
    System.out.println("Lock Test 1: two clients try to update the same item with different coordinator: one or both should fail");

    // update item
    c1.tell(new ClientMessage.Update(new Item(15, "UPDATE_LOCK1_N1"), n1), ActorRef.noSender());
    c2.tell(new ClientMessage.Update(new Item(15, "UPDATE_LOCK1_N3"), n3), ActorRef.noSender());

    try { Thread.sleep(SLEEPTIMEFULL); }
    catch (InterruptedException e) { e.printStackTrace(); }

    System.out.println("========================================\n\n");

    System.out.println("========================================");
    System.out.println("Read updated item (conflict case)");

    // read updated item
    c2.tell(new ClientMessage.Get(new Item(15, ""), n1), ActorRef.noSender());

    try { Thread.sleep(SLEEPTIMEFULL); }
    catch (InterruptedException e) { e.printStackTrace(); }

    System.out.println("========================================\n\n");

    System.out.println("========================================");
    System.out.println("Lock Test 2: two clients try to update the same item with same coordinator: one or both should fail");

    // update item
    c2.tell(new ClientMessage.Update(new Item(15, "UPDATE_LOCK2_N3_CL2"), n3), ActorRef.noSender());
    c1.tell(new ClientMessage.Update(new Item(15, "UPDATE_LOCK2_N3_CL1"), n3), ActorRef.noSender());

    try { Thread.sleep(SLEEPTIMEFULL); }
    catch (InterruptedException e) { e.printStackTrace(); }

    System.out.println("========================================\n\n");

    System.out.println("========================================");
    System.out.println("Read updated item (conflict case)");

    // read updated item
    c3.tell(new ClientMessage.Get(new Item(15, ""), n1), ActorRef.noSender());

    try { Thread.sleep(SLEEPTIMEFULL); }
    catch (InterruptedException e) { e.printStackTrace(); }

    System.out.println("========================================\n\n");
    System.out.println("========================================");
    System.out.println("Create and join node n4(key: 10)");
    //...end step 2

    // 3. Join
    try { Thread.sleep(SLEEPTIMEFULL); }
    catch (InterruptedException e) { e.printStackTrace(); }

    ActorRef n4 = system.actorOf(Node.props(N, R, W, T),"n4");
    n4.tell(new Message.JoinMsg(10, n1), ActorRef.noSender());

    try { Thread.sleep(SLEEPTIMEFULL); }
    catch (InterruptedException e) { e.printStackTrace(); }

    System.out.println("========================================\n\n");
    System.out.println("========================================");
    System.out.println("Create and join node n5(key: 50)");

    ActorRef n5 = system.actorOf(Node.props(N, R, W, T),"n5");
    n5.tell(new Message.JoinMsg(50, n3), ActorRef.noSender());    
    // ...end join

    try { Thread.sleep(SLEEPTIMEFULL); }
    catch (InterruptedException e) { e.printStackTrace(); }

    System.out.println("========================================\n\n");
    System.out.println("========================================");
    System.out.println("Lock Test 3: two clients try to update (item not present) the same item with different coordinator: one or both should fail");

    c2.tell(new ClientMessage.Update(new Item(49, "UPDATE_49_N1"), n1), ActorRef.noSender());
    c1.tell(new ClientMessage.Update(new Item(49, "UPDATE_49_N2"), n2), ActorRef.noSender());

    try { Thread.sleep(SLEEPTIMEFULL); }
    catch (InterruptedException e) { e.printStackTrace(); }

    System.out.println("========================================\n\n");
    System.out.println("========================================");
    System.out.println("Get updated item (conflict case)");

    c2.tell(new ClientMessage.Get(new Item(49, ""), n3), ActorRef.noSender());

    try { Thread.sleep(SLEEPTIMEFULL); }
    catch (InterruptedException e) { e.printStackTrace(); }

    // print item set of the nodes
    System.out.println("========================================\n\n");
    System.out.println("========================================");
    System.out.println("Print item list of all nodes");

    n1.tell(new Message.PrintItemList(), ActorRef.noSender());
    n2.tell(new Message.PrintItemList(), ActorRef.noSender());
    n3.tell(new Message.PrintItemList(), ActorRef.noSender());
    n4.tell(new Message.PrintItemList(), ActorRef.noSender());
    n5.tell(new Message.PrintItemList(), ActorRef.noSender());

    // ...end print item set of the nodes
    try { Thread.sleep(SLEEPTIMESHORT); }
    catch (InterruptedException e) { e.printStackTrace(); }

    // print item set of the nodes
    System.out.println("========================================\n\n");
    System.out.println("========================================");
    System.out.println("Node1 (key:20) leaving");

    //// leave n1
    n1.tell(new Message.LeaveMsg(), ActorRef.noSender());

    // print item set of the nodes
    try { Thread.sleep(SLEEPTIMEFULL); }
    catch (InterruptedException e) { e.printStackTrace(); }

    // print item set of the nodes
    System.out.println("========================================\n\n");
    System.out.println("========================================");
    System.out.println("Print item of each node after node1 leaves");

    n2.tell(new Message.PrintItemList(), ActorRef.noSender());
    n3.tell(new Message.PrintItemList(), ActorRef.noSender());
    n4.tell(new Message.PrintItemList(), ActorRef.noSender());
    n5.tell(new Message.PrintItemList(), ActorRef.noSender());

    // ...end print item set of the nodes
    try { Thread.sleep(SLEEPTIMESHORT); }
    catch (InterruptedException e) { e.printStackTrace(); }

    // print item set of the nodes
    System.out.println("========================================\n\n");
    System.out.println("========================================");
    System.out.println("Node2 (key:30) leaving");

    //// leave n2
    n2.tell(new Message.LeaveMsg(), ActorRef.noSender());

    // print item set of the nodes
    try { Thread.sleep(SLEEPTIMEFULL); }
    catch (InterruptedException e) { e.printStackTrace(); }

    // print item set of the nodes
    System.out.println("========================================\n\n");
    System.out.println("========================================");
    System.out.println("Print item of each node after node2 leaves");

    n3.tell(new Message.PrintItemList(), ActorRef.noSender());
    n4.tell(new Message.PrintItemList(), ActorRef.noSender());
    n5.tell(new Message.PrintItemList(), ActorRef.noSender());
    // ...end print item set of the nodes

    // ...end print item set of the nodes
    try { Thread.sleep(SLEEPTIMESHORT); }
    catch (InterruptedException e) { e.printStackTrace(); }

    // print item set of the nodes
    System.out.println("========================================\n\n");
    System.out.println("========================================");
    System.out.println("Node3 (key:40) leaving");
    //// leave n3
    n3.tell(new Message.LeaveMsg(), ActorRef.noSender());

    // print item set of the nodes
    try { Thread.sleep(SLEEPTIMEFULL); }
    catch (InterruptedException e) { e.printStackTrace(); }

    // print item set of the nodes
    System.out.println("========================================\n\n");
    System.out.println("========================================");
    System.out.println("Print item of each node after node2 leaves");

    n4.tell(new Message.PrintItemList(), ActorRef.noSender());
    n5.tell(new Message.PrintItemList(), ActorRef.noSender());
    // ...end print item set of the nodes

    try { Thread.sleep(SLEEPTIMESHORT); }
    catch (InterruptedException e) { e.printStackTrace(); }

    // print item set of the nodes
    System.out.println("========================================\n\n");
    System.out.println("========================================");
    System.out.println("Node4 (key:10) leaving");
    //// leave n4
    n4.tell(new Message.LeaveMsg(), ActorRef.noSender());

    // print item set of the nodes
    try { Thread.sleep(SLEEPTIMEFULL); }
    catch (InterruptedException e) { e.printStackTrace(); }

    // print item set of the nodes
    System.out.println("========================================\n\n");
    System.out.println("========================================");
    System.out.println("Print item of each node after node4 leaves");

    n5.tell(new Message.PrintItemList(), ActorRef.noSender());
    // ...end print item set of the nodes

    try { Thread.sleep(SLEEPTIMESHORT); }
    catch (InterruptedException e) { e.printStackTrace(); }

    // ... end leaving

    // test crash and recovery

    System.out.println("========================================\n\n");
    System.out.println("========================================");
    System.out.println("Create node6 (key:40) and join");
    //// create node n6
    ActorRef n6 = system.actorOf(Node.props(N, R, W, T),"n6");
    //// send to n6 the message to allow it joininig the network
    n6.tell(new Message.JoinMsg(40, n5), ActorRef.noSender());

    try { Thread.sleep(SLEEPTIMEFULL); }
    catch (InterruptedException e) { e.printStackTrace(); }

    System.out.println("========================================\n\n");
    System.out.println("========================================");
    System.out.println("Node6 prints the node list");
    n6.tell(new Message.PrintNodeList(), ActorRef.noSender());  // ask to print the current list of peers
    
    // print item set of the nodes
    try { Thread.sleep(SLEEPTIMESHORT); }
    catch (InterruptedException e) { e.printStackTrace(); }
    System.out.println("========================================\n\n");
    System.out.println("========================================");
    System.out.println("Node5 and 6 print the ITEM list");
    n5.tell(new Message.PrintItemList(), ActorRef.noSender());
    n6.tell(new Message.PrintItemList(), ActorRef.noSender());
    // ...end print item set of the nodes

    try { Thread.sleep(SLEEPTIMESHORT); }
    catch (InterruptedException e) { e.printStackTrace(); }
    System.out.println("========================================\n\n");
    System.out.println("========================================");
    System.out.println("Create node7 (key:65) and join");

    //// create node n7
    ActorRef n7 = system.actorOf(Node.props(N, R, W, T),"n7");

    //// send to n7 the message to allow it joininig the network
    n7.tell(new Message.JoinMsg(65, n5), ActorRef.noSender());

    try { Thread.sleep(SLEEPTIMEFULL); }
    catch (InterruptedException e) { e.printStackTrace(); }

    System.out.println("========================================\n\n");
    System.out.println("========================================");
    System.out.println("Node5 and node7 prints the ITEM list");
    n5.tell(new Message.PrintItemList(), ActorRef.noSender());
    n7.tell(new Message.PrintItemList(), ActorRef.noSender());
    // ...end print item set of the nodes

    ///// node n6 crashes
    try { Thread.sleep(SLEEPTIMESHORT); }
    catch (InterruptedException e) { e.printStackTrace(); }
    System.out.println("========================================\n\n");
    System.out.println("========================================");
    System.out.println("Node6 crashes");
    n6.tell(new Message.CrashMsg(), ActorRef.noSender());

    try { Thread.sleep(SLEEPTIMESHORT); }
    catch (InterruptedException e) { e.printStackTrace(); }
    System.out.println("========================================\n\n");
    System.out.println("========================================");
    System.out.println("Node5 prints the ITEM list");
    n5.tell(new Message.PrintItemList(), ActorRef.noSender());
    // ...end print item set of the nodes

    try { Thread.sleep(SLEEPTIMESHORT); }
    catch (InterruptedException e) { e.printStackTrace(); }
    System.out.println("========================================\n\n");
    System.out.println("========================================");
    System.out.println("Update and insert other elements");
    // write new items
    c1.tell(new ClientMessage.Update(new Item(39, "VALUE39"), n5), ActorRef.noSender());
    c2.tell(new ClientMessage.Update(new Item(48, "VALUE48"), n5), ActorRef.noSender());
    c3.tell(new ClientMessage.Update(new Item(64, "VALUE64"), n5), ActorRef.noSender());

    //// node n6 recovery
    try { Thread.sleep(SLEEPTIMEFULL); }
    catch (InterruptedException e) { e.printStackTrace(); }
    System.out.println("========================================\n\n");
    System.out.println("========================================");
    System.out.println("Node6 recovery");
    n6.tell(new Message.RecoveryMsg(n5), Actor.noSender());

    // print item set of the nodes
    try { Thread.sleep(SLEEPTIMEFULL); }
    catch (InterruptedException e) { e.printStackTrace(); }
    System.out.println("========================================\n\n");
    System.out.println("========================================");
    System.out.println("Node5, Node6 and Node7 print item list");
    n5.tell(new Message.PrintItemList(), ActorRef.noSender());
    n6.tell(new Message.PrintItemList(), ActorRef.noSender());
    n7.tell(new Message.PrintItemList(), ActorRef.noSender());
    // ...end print item set of the nodes

    // ... end test crash and recovery

    // print item set of the nodes
    try { Thread.sleep(SLEEPTIMESHORT); }
    catch (InterruptedException e) { e.printStackTrace(); }
    System.out.println("========================================\n\n");
    System.out.println("========================================");
    System.out.println("NEXT TO DO");

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
