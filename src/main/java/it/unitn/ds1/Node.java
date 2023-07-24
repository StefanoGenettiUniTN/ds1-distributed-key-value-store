package it.unitn.ds1;

import java.util.*;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

import java.util.concurrent.TimeUnit;
import scala.concurrent.duration.Duration;

public class Node extends AbstractActor {
  private final int key;                  // node key
  private final int N;
  private final int R;
  private final int W;
  private final int T;
  private int counterRequest;
  private final Map<Integer, ActorRef> peers;   // peers[K] points to the node in the group with key K
  private final Map<Integer, Item> items;       // the set of data item the node is currently responsible for
  private final Map<Integer, Request> requests; // Lists of the requests


  public Node(int _key, int n, int r, int w, int t){
    this.N = n;
    this.R = r;
    this.W = w;
    this.T = t;
    this.key = _key;
    this.peers = new TreeMap<>();
    this.items = new HashMap<>();
    this.requests = new HashMap<>();
    counterRequest = 0;
  }

  @Override
  public void preStart() {
    System.out.println("["+this.getSelf().path().name()+"] [preStart] Node key: "+this.key);
  }

  static public Props props(int _key, int n, int r, int w, int t) {
    return Props.create(Node.class, () -> new Node(_key, n, r, w, t));
  }

  // Mapping between the received message types and actor methods
  @Override
  public Receive createReceive() {
    return receiveBuilder()
      .match(Message.Hello.class, this::onHello)
      .match(Message.PrintNodeList.class, this::onPrintNodeList)
      .match(Message.PrintItemList.class, this::onPrintItemList)
      .match(Message.InitSystem.class, this::onInit)
      .match(Message.JoinMsg.class, this::onJoinMsg)
      .match(Message.ReqActiveNodeList.class, this::onReqActiveNodeList)
      .match(Message.ResActiveNodeList.class, this::onResActiveNodeList)
      .match(Message.GetRequest.class, this::onGetRequest)
      .match(Message.Read.class, this::onRead)
      .match(Message.ReadItemInformation.class, this::onReadItemInformation)
      .match(Message.Version.class, this::onVersion)
      .match(Message.UpdateRequest.class, this::onUpdateRequest)
      .match(Message.UpdateVersion.class, this::onUpdateVersion)
      .match(Message.Write.class, this::onWrite)
      .match(Message.AnnouncePresence.class, this::onAnnouncePresence)
      .match(Message.CrashMsg.class, this::onCrashMsg)
      .match(Message.Timeout.class, this::onTimeout)
      .build();
  }

  // Crash state behaviour
  final AbstractActor.Receive crashed() {
    return receiveBuilder()
      .match(Message.RecoveryMsg.class, this::onRecoveryMsg)
      .match(Message.ResActiveNodeList.class, this::onResActiveNodeList_recovery)
      .matchAny(msg -> {
        System.out.println(getSelf().path().name() + " ignoring " + msg.getClass().getSimpleName() + " (crashed)");
      })
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

  /*----------CRASH----------*/
  private void crash() {
    getContext().become(crashed());
  }

  // Receive CrashMsg and go to crash state
  private void onCrashMsg(Message.CrashMsg msg){
    System.out.println("["+this.getSelf().path().name()+"] [onCrashMsg]");
    this.crash();
  }

  /*----------END CRASH----------*/

  /*----------RECOVERY----------*/
  private void recover(){
    getContext().become(createReceive());
  }

  // When a crashed node is started again, it:
  // i.   requests the current set of nodes from a node specified in the recovery request;
  // ii.  discard those items that are no longer under its responsability;
  // iii. obtain the items that are now under its responsability
  private void onRecoveryMsg(Message.RecoveryMsg msg){
    System.out.println("["+this.getSelf().path().name()+"] [onRecoveryMsg]");

    // retrive message data
    ActorRef bootstrappingPeer = msg.bootstrappingPeer;

    // clear current local knowledge of nodes currently active in the network
    this.peers.clear();

    // i. requests the current set of nodes from a node specified in the recovery request;
    bootstrappingPeer.tell(new Message.ReqActiveNodeList(), this.getSelf());
  }

  // The bootstrapping node send the current list of the active nodes
  // to the node which is recovering from crash
  private void onResActiveNodeList_recovery(Message.ResActiveNodeList msg){
    System.out.println("["+this.getSelf().path().name()+"] [onResActiveNodeList_recovery]");

    //// TODO: riflettere se si può riciclare il onResActiveNodeList quando non conterrà più l'announce e onResActiveNodeList_recovery non conterrà più il cambiamento di stato a non crash

    // retrive message data and initialize the list of peers
    for (Map.Entry<Integer, ActorRef> pair : msg.activeNodes.entrySet()) {
      this.peers.put(pair.getKey(), pair.getValue());
    }

    // the node add itself to the list of nodes currently active
    this.peers.put(this.key, this.getSelf());

    // exit crash state 
    this.recover();
  }

  /*----------END RECOVERY----------*/

  // TODO: onGet(key)
  // TODO: onUpdate(key, value)



  /*----------GET RESPONSIBLE NODES FOR AN ITEM----------*/

  private Set<Integer> getResponsibleNode(int key){
    Set<Integer> responsibleNode = new HashSet<>();
    int n = this.N;

    for (Map.Entry<Integer, ActorRef> entry : peers.entrySet()) {
      if(n > 0 && key < entry.getKey()){
        responsibleNode.add(entry.getKey());
        n--;
      }

      if(n <= 0){
        break;
      }
    }

    if(n > 0){
      for (Map.Entry<Integer, ActorRef> entry : peers.entrySet()) {
        if(n > 0){
          responsibleNode.add(entry.getKey());
          n--;
        } else {
          break;
        }
      }
    }

    return responsibleNode;
  }

  /*----------END GET RESPONSIBLE NODES FOR AN ITEM----------*/

  /*----------GET----------*/

  // Coordinator manage get request
  private void onGetRequest(Message.GetRequest msg){
    int key = msg.key;
    System.out.println("["+this.getSelf().path().name()+"] [onGet] Coordinator");

    counterRequest++;
    this.requests.put(counterRequest, new Request(this.getSender(), new Item(key, ""), Type.GET));

    for(int node : getResponsibleNode(key)) {
      (peers.get(node)).tell(new Message.Read(counterRequest, key), this.getSelf());
    }

    //Timeout
    getContext().system().scheduler().scheduleOnce(
            Duration.create(Main.T, TimeUnit.SECONDS),
            this.getSelf(),
            new Message.Timeout(counterRequest), // the message to send,
            getContext().system().dispatcher(), this.getSelf()
    );
  }

  //Return information of an item to the coordinator
  private void onRead(Message.Read msg){
    int item_key = msg.key;
    Item item = this.items.get(item_key);
    if(item != null) {
      System.out.println("[" + this.getSelf().path().name() + "] [onRead] Owner: " + key + " ITEM: " + item);
      this.getSender().tell(new Message.ReadItemInformation(msg.requestId, item), ActorRef.noSender());
    }
  }

  //Return information to the client
  private void onReadItemInformation(Message.ReadItemInformation msg){
    System.out.println("["+this.getSelf().path().name()+"] [onReadItemInformation] Owner");

    Request req = this.requests.remove(msg.requestId);

    if(req != null) {
      req.setCounter(req.getCounter() + 1);

      int nR = req.getCounter();

      if(nR < this.R){
          Item item = req.getItem();
          if(msg.item.getVersion() > item.getVersion()){
            item.setVersion(msg.item.getVersion());
            item.setValue(msg.item.getValue());
          }
      } else if(nR == this.R) {
        req.getClient().tell(new ClientMessage.GetResult(Result.SUCCESS, msg.item), ActorRef.noSender());
      }
    }
  }

  /*----------END GET----------*/

  /*----------UPDATE----------*/

  private void onTimeout(Message.Timeout msg){
    if(this.requests.containsKey(msg.requestId) == true){
      Request req = this.requests.remove(msg.requestId);
      if(req.getType() == Type.GET) {
        req.getClient().tell(new ClientMessage.GetResult(Result.ERROR, null), ActorRef.noSender());
      } else {
        req.getClient().tell(new ClientMessage.UpdateResult(Result.ERROR, null), ActorRef.noSender());
      }
    }
  }

  // Coordinator retrieve last version
  private void onUpdateRequest(Message.UpdateRequest msg){
    Item item = new Item(msg.item);
    System.out.println("["+this.getSelf().path().name()+"] [onUpdate] Coordinator");

    this.counterRequest++;
    this.requests.put(counterRequest, new Request(this.getSender(), item, Type.UPDATE));

    for(int node : getResponsibleNode(item.key)) {
      (peers.get(node)).tell(new Message.Version(counterRequest, item), this.getSelf());
    }

    //Timeout
    getContext().system().scheduler().scheduleOnce(
          Duration.create(Main.T, TimeUnit.SECONDS),
          this.getSelf(),
          new Message.Timeout(counterRequest), // the message to send,
          getContext().system().dispatcher(), this.getSelf()
      );
  }

  //Return version of an item
  private void onVersion(Message.Version msg){
    Item item = new Item(msg.item);
    System.out.println("["+this.getSelf().path().name()+"] [onVersion] Owner");
    if(this.items.get(item.getKey()) != null) {
      item.setVersion((this.items.get(item.getKey())).getVersion());
    }

    //this.getSender().tell(new Message.UpdateVersion(msg.requestId, item), ActorRef.noSender());
    this.getSender().tell(new Message.UpdateVersion(msg.requestId, item), this.getSelf());
  }

  private void onUpdateVersion(Message.UpdateVersion msg){
    Item item = new Item(msg.item);
    System.out.println("["+this.getSelf().path().name()+"] [onUpdateVersion] Coordinator");

    Request req = this.requests.get(msg.requestId);

    if(req != null) {
      req.setCounter(req.getCounter() + 1);
      Item itemReq = req.getItem();
      int nW = req.getCounter();


      if(nW < this.W){
        if(msg.item.getVersion() > itemReq.getVersion()){
          itemReq.setVersion(msg.item.getVersion());
        }
      } else if(nW == this.W) {
        itemReq.setVersion(itemReq.getVersion() + 1);
        for (int node : getResponsibleNode(item.getKey())) {
          (peers.get(node)).tell(new Message.Write(msg.requestId, itemReq), this.getSelf());
        }

        this.requests.remove(msg.requestId);
        req.getClient().tell(new ClientMessage.UpdateResult(Result.SUCCESS, itemReq), ActorRef.noSender());
      }
    }
  }

  //Update item and send element to the coordinator
  private void onWrite(Message.Write msg){
    Item item = new Item(msg.item);
    this.items.put(item.getKey(), item);
    System.out.println("["+this.getSelf().path().name()+"] [onWriteInformation] Owner: " + key + " ITEM: " + item);
  }

  /*----------END UPDATE----------*/

  // Print the list of nodes
  private void onPrintNodeList(Message.PrintNodeList msg){
    System.out.println("["+this.getSelf().path().name()+"] [onPrintNodeList]");
    System.out.println(this.peers);
  }

  // Print the list of Item
  private void onPrintItemList(Message.PrintItemList msg){
    System.out.println("["+this.getSelf().path().name()+"] [onPrintItemList]: " + this.items);
  }

  /*======================*/
}
