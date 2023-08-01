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
      System.out.println("Parameters N, W and R are set wrongly");
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
    System.out.println("Create Client c1");
    ActorRef c1 = system.actorOf(Client.props(),"c1");

    try { Thread.sleep(SLEEPTIMESHORT); }
    catch (InterruptedException e) { e.printStackTrace(); }


    System.out.println("========================================");
    System.out.println("Try to insert first item: it should fail since no enough nodes are in the ring");

    // perform write operations
    c1.tell(new ClientMessage.Update(new Item(6, "VALUE6"), n1), ActorRef.noSender());

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
    System.out.println("Create other clients");

    // 2. Create client nodes and perform read and write operations
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
    System.out.println("Try to insert other two items from the same client, one of the two should fail");

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
    c1.tell(new ClientMessage.Update(new Item(15, "UPDATE_LOCK1_C1"), n1), ActorRef.noSender());
    c2.tell(new ClientMessage.Update(new Item(15, "UPDATE_LOCK1_C2"), n3), ActorRef.noSender());

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
    System.out.println("Node4 (key:10) try to leave: it should fail since no enough nodes are in the ring");
    //// leave n4
    n4.tell(new Message.LeaveMsg(), ActorRef.noSender());

    // print item set of the nodes
    try { Thread.sleep(SLEEPTIMEFULL); }
    catch (InterruptedException e) { e.printStackTrace(); }

    // print item set of the nodes
    System.out.println("========================================\n\n");
    System.out.println("========================================");
    System.out.println("Print item of each node");

    n5.tell(new Message.PrintItemList(), ActorRef.noSender());
    n4.tell(new Message.PrintItemList(), ActorRef.noSender());

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
    System.out.println("Node4, Node5 and Node6 print the ITEM list");
    n4.tell(new Message.PrintItemList(), ActorRef.noSender());
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
    System.out.println("The node print the ITEM list");
    n4.tell(new Message.PrintItemList(), ActorRef.noSender());
    n5.tell(new Message.PrintItemList(), ActorRef.noSender());
    n6.tell(new Message.PrintItemList(), ActorRef.noSender());
    n7.tell(new Message.PrintItemList(), ActorRef.noSender());
    // ...end print item set of the nodes

    ///// node n6 crashes
    try { Thread.sleep(SLEEPTIMESHORT); }
    catch (InterruptedException e) { e.printStackTrace(); }
    System.out.println("========================================\n\n");
    System.out.println("========================================");
    System.out.println("Node6 crashes");
    n6.tell(new Message.CrashMsg(), ActorRef.noSender());

      // ...end print item set of the nodes

    try { Thread.sleep(SLEEPTIMESHORT); }
    catch (InterruptedException e) { e.printStackTrace(); }
    System.out.println("========================================\n\n");
    System.out.println("========================================");
    System.out.println("Update and insert other elements: 48 and 64 should succeed, while 39 should fail since node6, one of the responsible nodes, is crashed");
    // write new items
    c1.tell(new ClientMessage.Update(new Item(39, "VALUE39"), n5), ActorRef.noSender());
    c2.tell(new ClientMessage.Update(new Item(48, "VALUE48"), n4), ActorRef.noSender());
    c3.tell(new ClientMessage.Update(new Item(64, "VALUE64"), n7), ActorRef.noSender());

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
    System.out.println("Print item list for each node");
    n4.tell(new Message.PrintItemList(), ActorRef.noSender());
    n5.tell(new Message.PrintItemList(), ActorRef.noSender());
    n6.tell(new Message.PrintItemList(), ActorRef.noSender());
    n7.tell(new Message.PrintItemList(), ActorRef.noSender());
    // ...end print item set of the nodes

    // ... end test crash and recovery

    // print item set of the nodes
    try { Thread.sleep(SLEEPTIMESHORT); }
    catch (InterruptedException e) { e.printStackTrace(); }

    // test timeout reqActiveNodeList

    ///// node n7 crashes
    System.out.println("========================================\n\n");
    System.out.println("========================================");
    System.out.println("TEST TIMEOUT reqActiveNodeList (bootstrapping peer crashed)");
    System.out.println("========================================");
    System.out.println("Node7 crashes");
    n7.tell(new Message.CrashMsg(), ActorRef.noSender());

    try { Thread.sleep(SLEEPTIMESHORT); }
    catch (InterruptedException e) { e.printStackTrace(); }
    System.out.println("========================================\n\n");
    System.out.println("========================================");
    System.out.println("Node8 (key:64) creation and tries to join with bootstrapping peer node7: since node7 is crashed a timeout is expected");
    ///// join n8
    ActorRef n8 = system.actorOf(Node.props(N, R, W, T),"n8");
    n8.tell(new Message.JoinMsg(64, n7), ActorRef.noSender());

    try { Thread.sleep(SLEEPTIMEFULL); }
    catch (InterruptedException e) { e.printStackTrace(); }

    System.out.println("========================================\n\n");
    System.out.println("========================================");
    System.out.println("Node7 recovery");
    ///// node n7 recover
    n7.tell(new Message.RecoveryMsg(n5), ActorRef.noSender());

    try { Thread.sleep(SLEEPTIMEFULL); }
    catch (InterruptedException e) { e.printStackTrace(); }

    System.out.println("========================================\n\n");
    System.out.println("========================================");
    System.out.println("Node8 prints node and item list: empty expected since join has been aborted");
    //// n8 print current list of peers
    n8.tell(new Message.PrintNodeList(), ActorRef.noSender());  // ask to print the current list of peers
    n8.tell(new Message.PrintItemList(), ActorRef.noSender());
    // ...end print item set of the nodes

    // ... end test timeout reqActiveNodeList

    // test timeout reqDataItemsResponsibleFor

    try { Thread.sleep(SLEEPTIMESHORT); }
    catch (InterruptedException e) { e.printStackTrace(); }

    System.out.println("========================================");
    System.out.println("END TEST TIMEOUT reqActiveNodeList (bootstrapping peer crashed)");
    System.out.println("========================================\n\n");
    System.out.println("========================================");
    System.out.println("TEST TIMEOUT reqDataItemsResponsibleFor (responsible updated items node crashed)");
    System.out.println("========================================");
    System.out.println("Node 7 crashes");

    ///// node n7 crashes
    n7.tell(new Message.CrashMsg(), ActorRef.noSender());

    try { Thread.sleep(SLEEPTIMESHORT); }
    catch (InterruptedException e) { e.printStackTrace(); }

    System.out.println("========================================\n\n");
    System.out.println("========================================");
    System.out.println("Node8 joins with node5 as bootstrapping peer");
    ///// join n8
    n8.tell(new Message.JoinMsg(63, n5), ActorRef.noSender());

    try { Thread.sleep(SLEEPTIMEFULL); }
    catch (InterruptedException e) { e.printStackTrace(); }

    System.out.println("========================================\n\n");
    System.out.println("========================================");
    System.out.println("Node7 recovery");
    ///// node n7 recover
    n7.tell(new Message.RecoveryMsg(n5), ActorRef.noSender());
    
    try { Thread.sleep(SLEEPTIMEFULL); }
    catch (InterruptedException e) { e.printStackTrace(); }

    System.out.println("========================================\n\n");
    System.out.println("========================================");
    System.out.println("Node8 prints list of node and item");
    //// n8 print current list of peers
    n8.tell(new Message.PrintNodeList(), ActorRef.noSender());  // ask to print the current list of peers
    n8.tell(new Message.PrintItemList(), ActorRef.noSender());
    // ...end print item set of the nodes

    // ... end test timeout reqDataItemsResponsibleFor

    try { Thread.sleep(SLEEPTIMESHORT); }
    catch (InterruptedException e) { e.printStackTrace(); }

    System.out.println("========================================");
    System.out.println("END TEST TIMEOUT reqDataItemsResponsibleFor (bootstrapping peer crashed)");
    System.out.println("========================================\n\n");
    System.out.println("========================================");
    System.out.println("TEST TIMEOUT ANNOUNCE DEPARTURE");
    System.out.println("========================================");
    System.out.println("Node4 crashes");

    // test timeout AnnounceDeparture
    ///// node n4 crashes
    n4.tell(new Message.CrashMsg(), ActorRef.noSender());

    try { Thread.sleep(SLEEPTIMESHORT); }
    catch (InterruptedException e) { e.printStackTrace(); }
    System.out.println("========================================\n\n");
    System.out.println("========================================");
    System.out.println("Node7 try to leave but since n4, who becomes responsible for some items, is crashed, it will fail and the timeout expires");

    //// node n7 leave
    n7.tell(new Message.LeaveMsg(), ActorRef.noSender());

    try { Thread.sleep(SLEEPTIMEFULL); }
    catch (InterruptedException e) { e.printStackTrace(); }
    System.out.println("========================================\n\n");
    System.out.println("========================================");
    System.out.println("Node4 recovery");

    ///// node n6 recover
    n4.tell(new Message.RecoveryMsg(n5), ActorRef.noSender());

    try { Thread.sleep(SLEEPTIMEFULL); }
    catch (InterruptedException e) { e.printStackTrace(); }
    System.out.println("========================================\n\n");
    System.out.println("========================================");
    System.out.println("Node7 prints nodes and items list");
    //// n7 print current list of peers
    n7.tell(new Message.PrintNodeList(), ActorRef.noSender());  // ask to print the current list of peers
    n7.tell(new Message.PrintItemList(), ActorRef.noSender());
    // ...end print item set of the nodes

    // ... end test timeout AnnounceDeparture
    try { Thread.sleep(SLEEPTIMESHORT); }
    catch (InterruptedException e) { e.printStackTrace(); }

    System.out.println("========================================");
    System.out.println("END TEST TIMEOUT ANNOUNCE DEPARTURE");
    System.out.println("========================================\n\n");
    System.out.println("========================================");
    System.out.println("TEST TIMEOUT JoinReadOperationReq (reading from node updated items failed)");
    System.out.println("========================================");
    System.out.println("Node5 prints nodes and items lists");

    //// n5 print current list of peers
    n5.tell(new Message.PrintNodeList(), ActorRef.noSender());  // ask to print the current list of peers
    n5.tell(new Message.PrintItemList(), ActorRef.noSender());

    try { Thread.sleep(SLEEPTIMESHORT); }
    catch (InterruptedException e) { e.printStackTrace(); }

    System.out.println("========================================\n\n");
    System.out.println("========================================");
    System.out.println("Node5 crashes");
    //// node n5 crash
    n5.tell(new Message.CrashMsg(), ActorRef.noSender());

    try { Thread.sleep(SLEEPTIMESHORT); }
    catch (InterruptedException e) { e.printStackTrace(); }

    System.out.println("========================================\n\n");
    System.out.println("========================================");
    System.out.println("Node8 will join but it cannot read updated items since node5 has crashed");
    ///// join n8
    n8.tell(new Message.JoinMsg(38, n6), ActorRef.noSender());

    try { Thread.sleep(SLEEPTIMEFULL); }
    catch (InterruptedException e) { e.printStackTrace(); }

    System.out.println("========================================\n\n");
    System.out.println("========================================");
    System.out.println("Node5 recover");
    //// node n5 recover
    n5.tell(new Message.RecoveryMsg(n6), ActorRef.noSender());

    try { Thread.sleep(SLEEPTIMEFULL); }
    catch (InterruptedException e) { e.printStackTrace(); }

    System.out.println("========================================\n\n");
    System.out.println("========================================");
    System.out.println("Node8 prints items and nodes lists");

    //// n8 print current list of peers
    n8.tell(new Message.PrintNodeList(), ActorRef.noSender());  // ask to print the current list of peers
    n8.tell(new Message.PrintItemList(), ActorRef.noSender());

    try { Thread.sleep(SLEEPTIMESHORT); }
    catch (InterruptedException e) { e.printStackTrace(); }

    System.out.println("========================================");
    System.out.println("END TEST TIMEOUT JoinReadOperationReq (reading from node updated items failed)");
    System.out.println("========================================\n\n");
    System.out.println("========================================");
    System.out.println("TEST TIMEOUT ReqActiveNodeList_recover (bootstrapping peer to recovery is crashed)");
    System.out.println("========================================");
    System.out.println("Node5 and node6 crashes");

    //// node n5 crash
    n5.tell(new Message.CrashMsg(), ActorRef.noSender());
    //// node n6 crash
    n6.tell(new Message.CrashMsg(), ActorRef.noSender());

    try { Thread.sleep(SLEEPTIMESHORT); }
    catch (InterruptedException e) { e.printStackTrace(); }

    System.out.println("========================================\n\n");
    System.out.println("========================================");
    System.out.println("Node6 try to recover but node5, the bootstrapping peer, is crashed, so it should fail");

    //// node n6 recover
    n6.tell(new Message.RecoveryMsg(n5), ActorRef.noSender());

    try { Thread.sleep(SLEEPTIMEFULL); }
    catch (InterruptedException e) { e.printStackTrace(); }

    System.out.println("========================================\n\n");
    System.out.println("========================================");
    System.out.println("Node5 recovery");
    //// node n5 recover
    n5.tell(new Message.RecoveryMsg(n7), ActorRef.noSender());

    try { Thread.sleep(SLEEPTIMEFULL); }
    catch (InterruptedException e) { e.printStackTrace(); }
    System.out.println("========================================\n\n");
    System.out.println("========================================");
    System.out.println("Node6 retry to recover and succeeds");

    //// node n6 recover
    n6.tell(new Message.RecoveryMsg(n5), ActorRef.noSender());

    try { Thread.sleep(SLEEPTIMEFULL); }
    catch (InterruptedException e) { e.printStackTrace(); }
    System.out.println("========================================\n\n");
    System.out.println("========================================");
    System.out.println("Node6 prints nodes and items lists");

    //// n6 print current list of peers
    n6.tell(new Message.PrintNodeList(), ActorRef.noSender());  // ask to print the current list of peers
    n6.tell(new Message.PrintItemList(), ActorRef.noSender());
    // ...end print item set of the nodes

    // ... end test timeout ReqActiveNodeList_recover
    try { Thread.sleep(SLEEPTIMESHORT); }
    catch (InterruptedException e) { e.printStackTrace(); }

    System.out.println("========================================");
    System.out.println("END TEST TIMEOUT ReqActiveNodeList_recover (bootstrapping peer to recovery is crashed)");
    System.out.println("========================================\n\n");
    System.out.println("========================================");
    System.out.println("TEST TIMEOUT ReqDataItemsResponsibleFor_recovery (nodes responsible for updated items are crashed)");
    System.out.println("========================================");
    System.out.println("Node4 and node5 crash");

    //// node n5 crash
    n5.tell(new Message.CrashMsg(), ActorRef.noSender());
    //// node n7 crash
    n7.tell(new Message.CrashMsg(), ActorRef.noSender());

    try { Thread.sleep(SLEEPTIMESHORT); }
    catch (InterruptedException e) { e.printStackTrace(); }
    System.out.println("========================================\n\n");
    System.out.println("========================================");
    System.out.println("Node5 try to recover but node7, who is responsible for some of his updated items, prints nodes and items lists");

    //// node n5 recover
    n5.tell(new Message.RecoveryMsg(n6), ActorRef.noSender());

    try { Thread.sleep(SLEEPTIMEFULL); }
    catch (InterruptedException e) { e.printStackTrace(); }
    System.out.println("========================================\n\n");
    System.out.println("========================================");
    System.out.println("Node7 recover");

    //// node n7 recover
    n7.tell(new Message.RecoveryMsg(n6), ActorRef.noSender());
    
    try { Thread.sleep(SLEEPTIMEFULL); }
    catch (InterruptedException e) { e.printStackTrace(); }
    System.out.println("========================================\n\n");
    System.out.println("========================================");
    System.out.println("Node5 retry to recover and succeeds");

    //// node n5 recover
    n5.tell(new Message.RecoveryMsg(n6), ActorRef.noSender());

    try { Thread.sleep(SLEEPTIMEFULL); }
    catch (InterruptedException e) { e.printStackTrace(); }
    System.out.println("========================================\n\n");
    System.out.println("========================================");
    System.out.println("Node5 prints nodes and items lists");

    //// n6 print current list of peers
    n5.tell(new Message.PrintNodeList(), ActorRef.noSender());  // ask to print the current list of peers
    n5.tell(new Message.PrintItemList(), ActorRef.noSender());

    try { Thread.sleep(SLEEPTIMESHORT); }
    catch (InterruptedException e) { e.printStackTrace(); }
    System.out.println("========================================");
    System.out.println("END TEST TIMEOUT ReqDataItemsResponsibleFor_recovery (nodes responsible for updated items are crashed)");
    System.out.println("========================================\n\n");

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
