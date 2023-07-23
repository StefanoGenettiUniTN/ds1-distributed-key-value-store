package it.unitn.ds1;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

public class Node extends AbstractActor {
  private final int key;                  // node key
  private final Map<Integer, ActorRef> peers;   // peers[K] points to the node in the group with key K
  private final Map<Integer, Item> items;       // the set of data item the node is currently responsible for

  public Node(int _key){
    this.key = _key;
    this.peers = new TreeMap<>();
    this.items = new HashMap<>();
  }

  @Override
  public void preStart() {
    System.out.println("["+this.getSelf().path().name()+"] [preStart] Node key: "+this.key);
  }

  static public Props props(int _key) {
    return Props.create(Node.class, () -> new Node(_key));
  }

  // Mapping between the received message types and actor methods
  @Override
  public Receive createReceive() {
    return receiveBuilder()
      .match(Message.Hello.class, this::onHello)
      .match(Message.PrintNodeList.class, this::onPrintNodeList)
      .match(Message.InitSystem.class, this::onInit)
      .match(Message.JoinMsg.class, this::onJoinMsg)
      .match(Message.ReqActiveNodeList.class, this::onReqActiveNodeList)
      .match(Message.ResActiveNodeList.class, this::onResActiveNodeList)
      .match(Message.AnnouncePresence.class, this::onAnnouncePresence)
      .build();
  }

  /*===MESSAGE HANDLERS===*/
  
  // Here we define our reaction on the received Hello messages
  private void onHello(Message.Hello h) {
    System.out.println("[" +
            getSelf().path().name() +      // the name of the current actor
            "] received a message from " +
            getSender().path().name() +    // the name of the sender actor
            ": " + h.msg                   // finally the message contents
    );
  }

  // First node in the network receives this message and
  // initializes the system adding itself to the group
  private void onInit(Message.InitSystem msg){
    System.out.println("["+this.getSelf().path().name()+"] [InitSystem] Node key: "+this.key);
    this.peers.put(this.key, this.getSelf());
  }

  // Node receive the JoinMsg from Main
  private void onJoinMsg(Message.JoinMsg msg){
    System.out.println("["+this.getSelf().path().name()+"] [onJoinMsg]");

    // retrive message data
    int msg_key = msg.key;
    ActorRef msg_bootstrappingPeer = msg.bootstrappingPeer;

    // ask to the bootstrapping peer the current list of active nodes
    Message.ReqActiveNodeList reqActiveNodeListMsg = new Message.ReqActiveNodeList();
    msg_bootstrappingPeer.tell(reqActiveNodeListMsg, this.getSelf());
  }

  // Bootrstrapping node is receiving this message from a node which is
  // requesting to join the network. The boostrap node respondes with
  // the list of currently active nodes in the system
  private void onReqActiveNodeList(Message.ReqActiveNodeList msg){
    System.out.println("["+this.getSelf().path().name()+"] [onReqActiveNodeList]");

    // send the list of currently active nodes
    Map<Integer, ActorRef> activeNodes = Collections.unmodifiableMap(this.peers);
    Message.ResActiveNodeList msg_response = new Message.ResActiveNodeList(activeNodes);
    this.getSender().tell(msg_response, this.getSelf());
  }

  // The node which is joining the network receives the current list
  // of active nodes from the bootstrapping node
  private void onResActiveNodeList(Message.ResActiveNodeList msg){
    System.out.println("["+this.getSelf().path().name()+"] [onResActiveNodeList]");

    // retrive message data and initialize the list of peers
    for (Map.Entry<Integer, ActorRef> pair : msg.activeNodes.entrySet()) {
      this.peers.put(pair.getKey(), pair.getValue());
    }

    // the node add itself to the list of nodes currently active
    this.peers.put(this.key, this.getSelf());

    // the node can finally announce its presence to every node in the system
    Message.AnnouncePresence announcePresence = new Message.AnnouncePresence(this.key);
    this.peers.forEach((k, p) -> {
      if(!p.equals(this.getSelf())){
        p.tell(announcePresence, this.getSelf());
      }
    });
  }

  // Sender of this message is a node which is joining the system.
  // It is announcing its presence to every node in the system.
  private void onAnnouncePresence(Message.AnnouncePresence msg){
    System.out.println("["+this.getSelf().path().name()+"] [onAnnouncePresence]");

    // retrive message data
    int msg_key = msg.key;  // the key of the new node which is asking to join the system

    // add the new node to the current list of active nodes
    this.peers.put(msg_key, this.getSender());
  }

  // TODO: onGet(key)

  // TODO: onUpdate(key, value)

  // Print the list of nodes
  private void onPrintNodeList(Message.PrintNodeList msg){
    System.out.println("["+this.getSelf().path().name()+"] [onPrintNodeList]");
    System.out.println(this.peers);
  }

  /*======================*/
}
