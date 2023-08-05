package it.unitn.ds1;
import java.io.IOException;

import akka.actor.Actor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;


/*
    AUTHORS: Carlin Nicola and Stefano Genetti

    DESCRIPTION: this project represents a distributed system which implements a distributed hash table based
      peer-to-peer key-value storage service inspired by Amazon Dynamo.
      The architecture is composed by two Akka actor classes whose instances are referred to as nodes
      and clients which interacts by means of exchange of messages. In order to simulate the behaviour
      of a real computer network, we add random delays before each transmission.
      The distributed hash table is composed by multiple peer nodes interconnected together. The stored
      data is partitioned among these nodes in order to balance the load. Symmetrically, several nodes
      record the same items for reliability and accessibility. The partitioning is based on the keys that
      are associated with both the stored items and the nodes. We consider only unsigned integers as
      keys, which are logically arranged in a circular space, such that the largest key value wraps around
      to the smallest key value like minutes on analog clocks.
      On the other hand, the clients support data services which consist of two commands: i. update(key,
      value); ii. get(key)→value; which are used respectively to insert and read from the DHT. Any
      storage node in the network is able to fulfill both requests regardless of the key, forwarding data
      to/from appropriate nodes. Although multiple clients can read and write on the data structure in
      parallel, we assume that each client performs read and write operations sequentially one at a time
      The overlay ring network topology of nodes which constitutes the distributed hash table, is not
      static; on the contrary nodes can dynamically join, leave, crash and recover one at a time and
      only when there are no ongoing operations. We assume that operations might resume while
      one or more nodes are still in crashed state. In order to handle these functionalities, each node
      supports management services which can be accessed by means of dedicated messages. When
      nodes leave or join the network, the system repartitions the data items accordingly.



    EXECUTION:  the execution1, that is started by default, shows a general and complete test of the project.
      It is possible to see a specific test, by uncommenting execution2, on how the replication with W quorum works.
      It is recommended to execute only one example at time since the output is very long.

 */


public class Main {

  final static int T = 5; // timeout
  final static int SLEEPTIMESHORT = 3000;
  final static int SLEEPTIMEFULL = 2 * T * 1000;

  public static void main(String[] args) {
    //execution1(2,2,2); //Complete example
    execution2(5,4,2); //Specific example of W quorum

    try {
      System.out.println(">>> Press ENTER to exit <<<");
      System.in.read();
    }
    catch (IOException ioe) {}

  }

  private static void execution1(int n, int w, int r){
    System.out.println("========================================");
    System.out.println("START EXECUTION 1");
    System.out.println("========================================\n\n");

    ActorSystem system = ActorSystem.create("ds1-project-first-example");
    int N = n; // degree of replication
    int W = w; // write quorum
    int R = r; // read quorum

    // compilation time constants are not properly set
    if(R + W <= N || W <= N/2){
      System.out.println("Parameters N, W and R are set wrongly");
      return;
    }

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
    System.out.println("Lock Test 1: two clients try to update the same item with different coordinator: none, one or both may fail depending on delay");

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
    System.out.println("Lock Test 2: two clients try to update the same item with same coordinator: none, one or both may fail depending on delay");

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
    System.out.println("Lock Test 3: two clients try to update (item not present) the same item with different coordinator: none, one or both may fail depending on delay");

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
    System.out.println("Node5 and node7 crash");

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
    system.terminate();

    System.out.println("========================================\n\n");
    System.out.println("========================================");
    System.out.println("END EXECUTION 1");
    System.out.println("========================================\n\n");
  }

  private static void execution2(int n, int w, int r) {
    System.out.println("========================================");
    System.out.println("START EXECUTION 2");
    System.out.println("========================================\n\n");

    ActorSystem system = ActorSystem.create("ds1-project-second-example");
    int N = n; // degree of replication
    int W = w; // write quorum
    int R = r; // read quorum

    // compilation time constants are not properly set
    if (R + W <= N || W <= N / 2) {
      System.out.println("Parameters N, W and R are set wrongly");
      return;
    }

    // 1. Create node group
    System.out.println("========================================");
    System.out.println("Create Node n1(key:10) and join (first node in the ring); finally print the list of active node");
    //// create node n1
    ActorRef n1 = system.actorOf(Node.props(N, R, W, T), "n1");

    //// send init system to n1
    n1.tell(new Message.InitSystem(10), ActorRef.noSender());

    try { Thread.sleep(SLEEPTIMEFULL); }
    catch (InterruptedException e) { e.printStackTrace(); }

    n1.tell(new Message.PrintNodeList(), ActorRef.noSender());  // ask to print the current list of peers

    try { Thread.sleep(SLEEPTIMESHORT); }
    catch (InterruptedException e) { e.printStackTrace(); }

    System.out.println("========================================\n\n");

    System.out.println("========================================");
    System.out.println("========================================");
    System.out.println("Create Nodes n2(key:20), n3(key:30), n4(key:40), n5(key:50) and n6(key:60)  and join; n1 finally prints the Node list");

    //// create nodes
    ActorRef n2 = system.actorOf(Node.props(N, R, W, T),"n2");
    ActorRef n3 = system.actorOf(Node.props(N, R, W, T),"n3");
    ActorRef n4 = system.actorOf(Node.props(N, R, W, T),"n4");
    ActorRef n5 = system.actorOf(Node.props(N, R, W, T),"n5");
    ActorRef n6 = system.actorOf(Node.props(N, R, W, T),"n6");

    //// send to the nodes the message to allow it joining the network
    n2.tell(new Message.JoinMsg(20, n1), ActorRef.noSender());
    try { Thread.sleep(SLEEPTIMEFULL); }
    catch (InterruptedException e) { e.printStackTrace(); }
    n3.tell(new Message.JoinMsg(30, n1), ActorRef.noSender());
    try { Thread.sleep(SLEEPTIMEFULL); }
    catch (InterruptedException e) { e.printStackTrace(); }
    n4.tell(new Message.JoinMsg(40, n1), ActorRef.noSender());
    try { Thread.sleep(SLEEPTIMEFULL); }
    catch (InterruptedException e) { e.printStackTrace(); }
    n5.tell(new Message.JoinMsg(50, n1), ActorRef.noSender());
    try { Thread.sleep(SLEEPTIMEFULL); }
    catch (InterruptedException e) { e.printStackTrace(); }
    n6.tell(new Message.JoinMsg(60, n1), ActorRef.noSender());

    try { Thread.sleep(SLEEPTIMEFULL); }
    catch (InterruptedException e) { e.printStackTrace(); }

    n1.tell(new Message.PrintNodeList(), ActorRef.noSender());  // ask to print the current list of peers
    n2.tell(new Message.PrintNodeList(), ActorRef.noSender());  // ask to print the current list of peers
    n3.tell(new Message.PrintNodeList(), ActorRef.noSender());  // ask to print the current list of peers
    n4.tell(new Message.PrintNodeList(), ActorRef.noSender());  // ask to print the current list of peers
    n5.tell(new Message.PrintNodeList(), ActorRef.noSender());  // ask to print the current list of peers
    n6.tell(new Message.PrintNodeList(), ActorRef.noSender());  // ask to print the current list of peers


    try { Thread.sleep(SLEEPTIMESHORT); }
    catch (InterruptedException e) { e.printStackTrace(); }

    System.out.println("========================================\n\n");
    System.out.println("========================================");
    System.out.println("Create Clients c1, c2 and c3");
    ActorRef c1 = system.actorOf(Client.props(),"c1");
    ActorRef c2 = system.actorOf(Client.props(),"c2");
    ActorRef c3 = system.actorOf(Client.props(),"c3");

    try { Thread.sleep(SLEEPTIMESHORT); }
    catch (InterruptedException e) { e.printStackTrace(); }

    System.out.println("========================================\n\n");
    System.out.println("========================================");
    System.out.println("Insert items in the system");

    // perform write operations
    c1.tell(new ClientMessage.Update(new Item(5, "VALUE5"), n3), ActorRef.noSender());
    c2.tell(new ClientMessage.Update(new Item(35, "VALUE35"), n5), ActorRef.noSender());
    c3.tell(new ClientMessage.Update(new Item(85, "VALUE85"), n1), ActorRef.noSender());

    try { Thread.sleep(SLEEPTIMEFULL); }
    catch (InterruptedException e) { e.printStackTrace(); }

    System.out.println("========================================\n\n");
    System.out.println("========================================");
    System.out.println("Insert other items in the system");

    // perform write operations
    c1.tell(new ClientMessage.Update(new Item(15, "VALUE15"), n2), ActorRef.noSender());
    c2.tell(new ClientMessage.Update(new Item(55, "VALUE55"), n5), ActorRef.noSender());
    c3.tell(new ClientMessage.Update(new Item(25, "VALUE25"), n6), ActorRef.noSender());

    try { Thread.sleep(SLEEPTIMEFULL); }
    catch (InterruptedException e) { e.printStackTrace(); }

    System.out.println("========================================\n\n");
    System.out.println("========================================");
    System.out.println("Each node prints the list of the items");

    n1.tell(new Message.PrintItemList(), ActorRef.noSender());  // ask to print the current list of item
    n2.tell(new Message.PrintItemList(), ActorRef.noSender());  // ask to print the current list of item
    n3.tell(new Message.PrintItemList(), ActorRef.noSender());  // ask to print the current list of item
    n4.tell(new Message.PrintItemList(), ActorRef.noSender());  // ask to print the current list of item
    n5.tell(new Message.PrintItemList(), ActorRef.noSender());  // ask to print the current list of item
    n6.tell(new Message.PrintItemList(), ActorRef.noSender());  // ask to print the current list of item

    try { Thread.sleep(SLEEPTIMESHORT); }
    catch (InterruptedException e) { e.printStackTrace(); }

    System.out.println("========================================\n\n");
    System.out.println("========================================");
    System.out.println("Node 4 crashes");

    n4.tell(new Message.CrashMsg(), ActorRef.noSender());

    try { Thread.sleep(SLEEPTIMESHORT); }
    catch (InterruptedException e) { e.printStackTrace(); }

    System.out.println("========================================\n\n");
    System.out.println("========================================");
    System.out.println("Try to insert element 38: expected to succeed. Then prints the list on items in all nodes");

    c2.tell(new ClientMessage.Update(new Item(38, "VALUE55"), n5), ActorRef.noSender());

    try { Thread.sleep(SLEEPTIMEFULL); }
    catch (InterruptedException e) { e.printStackTrace(); }

    System.out.println("========================================\n\n");

    n1.tell(new Message.PrintItemList(), ActorRef.noSender());  // ask to print the current list of item
    n2.tell(new Message.PrintItemList(), ActorRef.noSender());  // ask to print the current list of item
    n3.tell(new Message.PrintItemList(), ActorRef.noSender());  // ask to print the current list of item
    n4.tell(new Message.PrintItemList(), ActorRef.noSender());  // ask to print the current list of item
    n5.tell(new Message.PrintItemList(), ActorRef.noSender());  // ask to print the current list of item
    n6.tell(new Message.PrintItemList(), ActorRef.noSender());  // ask to print the current list of item

    try { Thread.sleep(SLEEPTIMESHORT); }
    catch (InterruptedException e) { e.printStackTrace(); }

    System.out.println("========================================\n\n");
    System.out.println("========================================");
    System.out.println("Node 5 crash");

    n5.tell(new Message.CrashMsg(), ActorRef.noSender());

    try { Thread.sleep(SLEEPTIMESHORT); }
    catch (InterruptedException e) { e.printStackTrace(); }

    System.out.println("========================================\n\n");
    System.out.println("========================================");
    System.out.println("Try to insert element 28 and 58: 28 should fail since no enough W replies can arrive while 58 should succeed. Then prints the list on items in all nodes");

    c2.tell(new ClientMessage.Update(new Item(28, "VALUE28"), n2), ActorRef.noSender());
    c1.tell(new ClientMessage.Update(new Item(58, "VALUE58"), n6), ActorRef.noSender());


    try { Thread.sleep(SLEEPTIMEFULL + SLEEPTIMESHORT); }
    catch (InterruptedException e) { e.printStackTrace(); }

    System.out.println("========================================\n\n");

    n1.tell(new Message.PrintItemList(), ActorRef.noSender());  // ask to print the current list of item
    n2.tell(new Message.PrintItemList(), ActorRef.noSender());  // ask to print the current list of item
    n3.tell(new Message.PrintItemList(), ActorRef.noSender());  // ask to print the current list of item
    n4.tell(new Message.PrintItemList(), ActorRef.noSender());  // ask to print the current list of item
    n5.tell(new Message.PrintItemList(), ActorRef.noSender());  // ask to print the current list of item
    n6.tell(new Message.PrintItemList(), ActorRef.noSender());  // ask to print the current list of item

    try { Thread.sleep(SLEEPTIMESHORT); }
    catch (InterruptedException e) { e.printStackTrace(); }

    System.out.println("========================================\n\n");
    System.out.println("========================================");
    System.out.println("Node 6 crash");

    n6.tell(new Message.CrashMsg(), ActorRef.noSender());

    try { Thread.sleep(SLEEPTIMESHORT); }
    catch (InterruptedException e) { e.printStackTrace(); }

    System.out.println("========================================\n\n");
    System.out.println("========================================");
    System.out.println("Try to insert element 1: W quorum cannot be reached, operations will fail. Then prints the list on items in all nodes");

    c2.tell(new ClientMessage.Update(new Item(1, "VALUE28"), n1), ActorRef.noSender());


    try { Thread.sleep(SLEEPTIMEFULL + SLEEPTIMESHORT); }
    catch (InterruptedException e) { e.printStackTrace(); }

    System.out.println("========================================\n\n");

    n1.tell(new Message.PrintItemList(), ActorRef.noSender());  // ask to print the current list of item
    n2.tell(new Message.PrintItemList(), ActorRef.noSender());  // ask to print the current list of item
    n3.tell(new Message.PrintItemList(), ActorRef.noSender());  // ask to print the current list of item
    n4.tell(new Message.PrintItemList(), ActorRef.noSender());  // ask to print the current list of item
    n5.tell(new Message.PrintItemList(), ActorRef.noSender());  // ask to print the current list of item
    n6.tell(new Message.PrintItemList(), ActorRef.noSender());  // ask to print the current list of item

    try { Thread.sleep(SLEEPTIMESHORT); }
    catch (InterruptedException e) { e.printStackTrace(); }

    System.out.println("========================================\n\n");
    System.out.println("========================================");
    System.out.println("Node 6 recovery");

    n6.tell(new Message.RecoveryMsg(n2), ActorRef.noSender());

    try { Thread.sleep(SLEEPTIMEFULL); }
    catch (InterruptedException e) { e.printStackTrace(); }

    System.out.println("========================================\n\n");
    System.out.println("========================================");
    System.out.println("Node 5 recovery");

    n5.tell(new Message.RecoveryMsg(n3), ActorRef.noSender());

    try { Thread.sleep(SLEEPTIMEFULL); }
    catch (InterruptedException e) { e.printStackTrace(); }

    System.out.println("========================================\n\n");
    System.out.println("========================================");
    System.out.println("Node 4 recovery");

    n4.tell(new Message.RecoveryMsg(n2), ActorRef.noSender());

    try { Thread.sleep(SLEEPTIMEFULL); }
    catch (InterruptedException e) { e.printStackTrace(); }

    System.out.println("========================================\n\n");
    System.out.println("========================================");
    System.out.println("Each node prints it items lists: the recovery nodes should have all them items for which they are responsible");

    n1.tell(new Message.PrintItemList(), ActorRef.noSender());  // ask to print the current list of item
    n2.tell(new Message.PrintItemList(), ActorRef.noSender());  // ask to print the current list of item
    n3.tell(new Message.PrintItemList(), ActorRef.noSender());  // ask to print the current list of item
    n4.tell(new Message.PrintItemList(), ActorRef.noSender());  // ask to print the current list of item
    n5.tell(new Message.PrintItemList(), ActorRef.noSender());  // ask to print the current list of item
    n6.tell(new Message.PrintItemList(), ActorRef.noSender());  // ask to print the current list of item

    try { Thread.sleep(SLEEPTIMESHORT); }
    catch (InterruptedException e) { e.printStackTrace(); }

    system.terminate();

    System.out.println("========================================\n\n");
    System.out.println("========================================");
    System.out.println("END EXECUTION 2");
    System.out.println("========================================\n\n");

  }
}
