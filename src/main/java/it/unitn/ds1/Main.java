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

    // 2. Create client node c1
    ActorRef c1 = system.actorOf(Client.props(),"c1");
    ActorRef c2 = system.actorOf(Client.props(),"c2");

    c1.tell(new ClientMessage.Update(new Item(35, "Ciao"), n1), ActorRef.noSender());
    c2.tell(new ClientMessage.Update(new Item(7, "Come"), n2), ActorRef.noSender());
    c1.tell(new ClientMessage.Update(new Item(58, "Va"), n3), ActorRef.noSender());

    try { Thread.sleep(1000); }
    catch (InterruptedException e) { e.printStackTrace(); }

    c1.tell(new ClientMessage.Get(7, n1), ActorRef.noSender());
    c1.tell(new ClientMessage.Get(35, n1), ActorRef.noSender());
    c1.tell(new ClientMessage.Get(58, n1), ActorRef.noSender());

    try { Thread.sleep(1000); }
    catch (InterruptedException e) { e.printStackTrace(); }

    c1.tell(new ClientMessage.Update(new Item(58, "CAMBIATO"), n1), ActorRef.noSender());

    try { Thread.sleep(1000); }
    catch (InterruptedException e) { e.printStackTrace(); }

    c2.tell(new ClientMessage.Get(58, n1), ActorRef.noSender());
    //...end step 2

    try { Thread.sleep(1000); }
    catch (InterruptedException e) { e.printStackTrace(); }

    n3.tell(new Message.PrintItemList(), ActorRef.noSender());  // ask to print the current list of peers

    n1.tell(new Message.PrintItemList(), ActorRef.noSender());  // ask to print the current list of peers

    n2.tell(new Message.PrintItemList(), ActorRef.noSender());  // ask to print the current list of peers

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
