package it.unitn.ds1;

import java.util.*;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.Cancellable;

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

  //----FLAGS----
  private int join_update_item_response_counter;  // the joining node performs read operations to ensure that its items are up to date.
                                                  // This attribute is the number of nodes from which it is waiting for the updated
                                                  // version of the items
  
  private boolean flag_reqActiveNodeList;
  private boolean flag_reqDataItemsResponsibleFor;

  //-------------

  public Node(int _key, int n, int r, int w, int t){
    this.N = n;
    this.R = r;
    this.W = w;
    this.T = t;
    this.key = _key;
    this.peers = new TreeMap<>();
    this.items = new HashMap<>();
    this.requests = new HashMap<>();
    this.counterRequest = 0;

    this.join_update_item_response_counter = 0;
    this.flag_reqActiveNodeList = false;
    this.flag_reqDataItemsResponsibleFor = false;
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
      .match(Message.ReqDataItemsResponsibleFor.class, this::onReqDataItemsResponsibleFor)
      .match(Message.ReqDataItemsResponsibleFor_recovery.class, this::onReqDataItemsResponsibleFor_recovery)
      .match(Message.ResDataItemsResponsibleFor.class, this::onResDataItemsResponsibleFor)
      .match(Message.JoinReadOperationReq.class, this::onJoinReadOperationReq)
      .match(Message.JoinReadOperationRes.class, this::onJoinReadOperationRes)
      .match(Message.AnnouncePresence.class, this::onAnnouncePresence)
      .match(Message.LeaveMsg.class, this::onLeaveMsg)
      .match(Message.AnnounceDeparture.class, this::onAnnounceDeparture)
      .match(Message.CrashMsg.class, this::onCrashMsg)
      .match(Message.Timeout.class, this::onTimeout)
      .match(Message.Timeout_ReqActiveNodeList.class, this::onTimeout_ReqActiveNodeList)
      .match(Message.Timeout_ReqDataItemsResponsibleFor.class, this::onTimeout_ReqDataItemsResponsibleFor)
      .build();
  }

  // Crash state behaviour
  final AbstractActor.Receive crashed() {
    return receiveBuilder()
      .match(Message.RecoveryMsg.class, this::onRecoveryMsg)
      .match(Message.ResActiveNodeList.class, this::onResActiveNodeList_recovery)
      .match(Message.ResDataItemsResponsibleFor.class, this::onResDataItemsResponsibleFor_recovery)
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

  /*----------JOIN----------*/

  // Node receive the JoinMsg from Main
  private void onJoinMsg(Message.JoinMsg msg){
    System.out.println("["+this.getSelf().path().name()+"] [onJoinMsg]");

    // retrive message data
    int msg_key = msg.key;
    ActorRef msg_bootstrappingPeer = msg.bootstrappingPeer;

    // ask to the bootstrapping peer the current list of active nodes
    this.flag_reqActiveNodeList = false;
    Message.ReqActiveNodeList reqActiveNodeListMsg = new Message.ReqActiveNodeList();
    msg_bootstrappingPeer.tell(reqActiveNodeListMsg, this.getSelf());

    // if the bootstrapping peer does not send a response before the timeout
    // we abort the join process
    getContext().system().scheduler().scheduleOnce(
      Duration.create(this.T, TimeUnit.SECONDS),  // after 1 second
      this.getSelf(),                             // destination actor reference
      new Message.Timeout_ReqActiveNodeList(),    // the message to send,
      getContext().system().dispatcher(),         // system dispatcher
      this.getSelf()                              // source of the message (myself)
    );
  }

  // the bootstrapping node is not sending a response
  // --> abort join operation
  private void onTimeout_ReqActiveNodeList(Message.Timeout_ReqActiveNodeList msg){  // TODO: se si vuole, potenzialmente di potrebbe gestire tutti i timeout con una sola funzione?
    if(this.flag_reqActiveNodeList == false){
      System.out.println("["+this.getSelf().path().name()+"] [onTimeout_ReqActiveNodeList] ABORT JOIN because no ResActiveNodeList has been received before timeout expiration.");
      // in this case, nothing has been done, we can simply do nothing
    }
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
    this.flag_reqActiveNodeList = true; // finally the ResActiveNodeList has been received, we can go on with the join operation without aborting
    System.out.println("["+this.getSelf().path().name()+"] [onResActiveNodeList]");

    // retrive message data and initialize the list of peers
    for (Map.Entry<Integer, ActorRef> pair : msg.activeNodes.entrySet()) {
      this.peers.put(pair.getKey(), pair.getValue());
    }

    // get clocwise neighbor which has to be queried to request data items 
    // the joining node is responsible for
    ActorRef clockwiseNeighbor = this.getClockwiseNeighbor();
    System.out.println("["+this.getSelf().path().name()+"] [onResActiveNodeList] My clockwise neighbour is: "+clockwiseNeighbor);

    // request data items the joining node is responsible for from its clockwise neighbor (which holds all items it needs)
    this.flag_reqDataItemsResponsibleFor = false;
    Message.ReqDataItemsResponsibleFor clockwiseNeighborRequest = new Message.ReqDataItemsResponsibleFor(this.key);
    clockwiseNeighbor.tell(clockwiseNeighborRequest, this.getSelf());

    // if the clocwise neighbour peer does not send a response before the timeout
    // we abort the join operation
    getContext().system().scheduler().scheduleOnce(
      Duration.create(this.T, TimeUnit.SECONDS),  // after 1 second
      this.getSelf(),                             // destination actor reference
      new Message.Timeout_ReqDataItemsResponsibleFor(),   // the message to send,
      getContext().system().dispatcher(),         // system dispatcher
      this.getSelf()                              // source of the message (myself)
    );
  }

  // the clockwise neighbour node is not sending a response
  // --> abort join operation
  private void onTimeout_ReqDataItemsResponsibleFor(Message.Timeout_ReqDataItemsResponsibleFor msg){
    if(this.flag_reqDataItemsResponsibleFor == false){
      System.out.println("["+this.getSelf().path().name()+"] [onTimeout_ReqDataItemsResponsibleFor] ABORT JOIN because no ResDataItemsResponsibleFor has been received before timeout expiration.");
      this.peers.clear();
    }
  }

  // receive this request from a joining node "jn" which is requesting to its clockwise neighbor
  // the data items "jn" should be responsible for.
  // The message msg contains the key of the joining node
  private void onReqDataItemsResponsibleFor(Message.ReqDataItemsResponsibleFor msg){
    System.out.println("["+this.getSelf().path().name()+"] [onReqDataItemsResponsibleFor]");

    // retrive message data
    Integer joiningNodeKey = msg.key;

    // iterate the data item set to find the data items the joining node is responsible for
    Set<Item> resSet = new HashSet<>();
    TreeSet<Integer> simulateNewRing = new TreeSet<>();
    simulateNewRing.addAll(this.peers.keySet());
    simulateNewRing.add(joiningNodeKey);
    for (Map.Entry<Integer, Item> dataItem : this.items.entrySet()) {
      if(this.getResponsibleNode(dataItem.getKey(), simulateNewRing).contains(joiningNodeKey)){
        resSet.add(dataItem.getValue());
      }
    }

    // send the list of data item that the joining node is responsible for
    Message.ResDataItemsResponsibleFor msg_response = new Message.ResDataItemsResponsibleFor(Collections.unmodifiableSet(resSet));
    this.getSender().tell(msg_response, this.getSelf());
  }

  // the joining node receives the set of data imtems it is responsible for from its clockwise neighbor
  private void onResDataItemsResponsibleFor(Message.ResDataItemsResponsibleFor msg){
    this.flag_reqDataItemsResponsibleFor = true; // finally the ResDataItemsResponsibleFor has been received, we can go on with the join operation
    System.out.println("["+this.getSelf().path().name()+"] [onResDataItemsResponsibleFor]");

    // retrive message data and add the data items the joining node is responsible for
    for(Item item : msg.resSet) {
      this.items.put(item.key, item); //TODO: fare copia valore e non riferimento
    }
    System.out.println("["+this.getSelf().path().name()+"] [onResDataItemsResponsibleFor] Now I am responsible for the following data items:"+this.items.values());

    if(!this.items.isEmpty()){

      //--- perform read operations to ensure that the received items are up to date. // TODO: riflettere sui timeout
      //--- remark: no read request is sent to the clockwise neighbor which has just sent the current set of items
      Set<ActorRef> readDestinationNodes = new HashSet<>();
      int n = this.N;

      ArrayList<Integer> peerKeyList = new ArrayList<Integer>(this.peers.keySet());
      int clockwiseNeighborIndex = -1;
      for(int counter = 0; counter<peerKeyList.size(); counter++){  // get index of the clockwise neighbour in the list of peers
        Integer pk = peerKeyList.get(counter);
        if(this.peers.get(pk).equals(this.getSender())){
          clockwiseNeighborIndex = counter;
          break;
        }
      }

      // send the read request to the N-1 nodes after the clockwise neighbor of the joining node 
      for(int i=1; i<=n-1; i++){
        readDestinationNodes.add( 
          this.peers.get(
            peerKeyList.get(
              (clockwiseNeighborIndex + i)%peerKeyList.size()
            )
          )
        );
      }

      // send the read request to the N-1 nodes before the clockwise neighbor of the joining node 
      for(int i=1; i<=n-1; i++){
        int pos = clockwiseNeighborIndex - i; 
        if(pos < 0){
          pos = peerKeyList.size() + pos;
        }
        readDestinationNodes.add( 
          this.peers.get(
            peerKeyList.get(pos)
          )
        );
      }

      // send read operation
      Set<Item> itemSet = new HashSet<>();
      for(Item item : this.items.values()){
        itemSet.add(item);
      }
      Message.JoinReadOperationReq msg_JoinReadOperationReq = new Message.JoinReadOperationReq(Collections.unmodifiableSet(itemSet));
      for(ActorRef dest : readDestinationNodes){
        if(!dest.equals(this.getSender())){  // no read request is sent to the clockwise neighbor which has just sent the current set of items TODO: oppure ci sta mandarlo anche a lui?
          dest.tell(msg_JoinReadOperationReq, this.getSelf());
          this.join_update_item_response_counter++;
        }
      }
      // TODO: riflettere se qui si può ridurre il numero di messaggi scambiati
      // inviando solo ai nodi che effettivamente possono essere i responsabili di un item
      //---
    }
    // in the case this.join_update_item_response_counter==0 then it is not necessary to perform any read operation
    // and the node can immediately announce its presence to every node in the system
    if(this.join_update_item_response_counter == 0){
      
      // the node add itself to the list of nodes currently active
      this.peers.put(this.key, this.getSelf());

      // the node can finally announce its presence to every node in the system
      Set<Integer> announcePresenceKeyItemSet = new HashSet<>(this.items.keySet());
      Message.AnnouncePresence announcePresence = new Message.AnnouncePresence(this.key, Collections.unmodifiableSet(announcePresenceKeyItemSet));
      this.peers.forEach((k, p) -> {
        if(!p.equals(this.getSelf())){
          p.tell(announcePresence, this.getSelf());
        }
      });
    }

  }

  // Receive this message from a node that in the context of the join operation, it is performing reads to ensure that
  // its items are up to date
  private void onJoinReadOperationReq(Message.JoinReadOperationReq msg){
    System.out.println("["+this.getSelf().path().name()+"] [onJoinReadOperationReq]");

    // retrive message data and collect those items which
    // have a higher version in this node
    Set<Item> updatedItems = new HashSet<>();
    for(Item item : msg.requestItemSet) {
      if(this.items.containsKey(item.getKey()) && this.items.get(item.getKey()).getVersion() > item.getVersion()){  // the current node has a more updated version of the item
        Item updatedItem = new Item(  item.key,
                                      this.items.get(item.getKey()).getValue(),
                                      this.items.get(item.getKey()).getVersion());
        updatedItems.add(updatedItem);
      }
    }

    // send the update version of the requested items
    // to the node which is joining the network
    Message.JoinReadOperationRes joinReadOperationResponse = new Message.JoinReadOperationRes(Collections.unmodifiableSet(updatedItems));
    this.getSender().tell(joinReadOperationResponse, this.getSelf());
  }

  // The joining node is receiving the updated version of its items from the other peers.
  // As soon as the joining node receives the JoinReadOperationRes from all the requested
  // nodes, the node can finally announce its presence to every node in the system and start
  // serving requests coming from clients.
  private void onJoinReadOperationRes(Message.JoinReadOperationRes msg){
    System.out.println("["+this.getSelf().path().name()+"] [onJoinReadOperationRes]");
    
    // decrease the counter of nodes from which I am waiting for
    // the updated version of the items
    this.join_update_item_response_counter--;

    // retrive message data and update the items in this.items accordingly
    for(Item item : msg.responseItemSet){
      if(this.items.get(item.getKey()).getVersion() < item.getVersion()){
        Item updatedItem = new Item(  item.key,
                                      item.getValue(),
                                      item.getVersion());
        this.items.put(updatedItem.getKey(), updatedItem);
      }
    }

    // all the requested nodes have sent the updated version of the items.
    // Finally, the present joining node can announce its presence to every
    // node in the system and start serving requests coming from clients.
    if(this.join_update_item_response_counter == 0){
      // the node add itself to the list of nodes currently active
      this.peers.put(this.key, this.getSelf());

      // the node can finally announce its presence to every node in the system
      Set<Integer> announcePresenceKeyItemSet = new HashSet<>(this.items.keySet());
      Message.AnnouncePresence announcePresence = new Message.AnnouncePresence(this.key, Collections.unmodifiableSet(announcePresenceKeyItemSet));
      this.peers.forEach((k, p) -> {
        if(!p.equals(this.getSelf())){
          p.tell(announcePresence, this.getSelf());
        }
      });
    }

  }

  // Sender of this message is a node which is joining the system.
  // It is announcing its presence to every node in the system.
  private void onAnnouncePresence(Message.AnnouncePresence msg){
    System.out.println("["+this.getSelf().path().name()+"] [onAnnouncePresence]");

    // retrive message data
    int msg_key = msg.key;  // the key of the new node which is asking to join the system
    Set<Integer> msg_keyItemSet = new HashSet<>();
    msg_keyItemSet.addAll(msg.keyItemSet);  // TODO: riflettere se è giusto così dal punto di vista dell'immutable final e quelle cose la, magari visto che è solo lettura, posso anche leggere dal messaggio simplicemente

    // add the new node to the current list of active nodes
    this.peers.put(msg_key, this.getSender());

    // for each item in keyItemSet which is also in this.items, check if
    // the present node is still responsible for. Consequently, remove the data items
    // the present node is no longer responsible for. 
    for(Integer itemKey : msg_keyItemSet){
      if(this.items.keySet().contains(itemKey)){
        if(!this.getResponsibleNode(itemKey).contains(this.key)){  // the present node is no more responsible for this item
          this.items.remove(itemKey); // remove the item
        }
      }
    }

  }

  /*----------END JOIN----------*/

  /*LEAVE*/

  // a node receives LeaveMsg from main that requests the node to leave
  private void onLeaveMsg(Message.LeaveMsg msg){
    System.out.println("["+this.getSelf().path().name()+"] [onLeaveMsg]"); 
    
    HashMap<Integer, HashSet<Integer>> responsibleNode = new HashMap<>(); // responsibleNode[k] :: list of item keys the node k is
                                                                          // responsible for after the departure of the present node
    this.peers.keySet().forEach((peerKey) -> {responsibleNode.put(peerKey, new HashSet<>());});  

    // fill the list of responsible node (responsibleNode) for each item in this.items
    for(Integer itemKey : this.items.keySet()){
      Set<Integer> responsibleNodeKeys = this.getResponsibleNode(itemKey);
      responsibleNodeKeys.forEach((respNodeKey) -> {
        if(respNodeKey != this.key){
          HashSet<Integer> respNodeSet = responsibleNode.get(respNodeKey);
          respNodeSet.add(itemKey);
        }
      });
    }

    // remove the present node from the list of active nodes
    this.peers.remove(this.key);                                                                          
    
    // after the removal of the present node we compute again the responsabilites for each item in this.items
    // and we keep only the differences, i.e. if the node was not responsible for this item before, but it becomes
    // responsible of the item only now that the present node left the ring
    for(Integer itemKey : this.items.keySet()){
      Set<Integer> responsibleNodeKeys = this.getResponsibleNode(itemKey);
      responsibleNodeKeys.forEach((respNodeKey) -> {
        if(responsibleNode.get(respNodeKey).contains(itemKey)){ // in this case the node was already resoponsible for the item even before the departure of the present node
          responsibleNode.get(respNodeKey).remove(itemKey);
        }else{
          responsibleNode.get(respNodeKey).add(itemKey);
        }
      });
    }

    // the node announces to every other node that it is leaving
    // the node passes its data items to the nodes that become responsible for them after its departure
    this.peers.forEach((k, p) -> {

      // prepare announce departure message
      Set<Item> announceDepartureSet = new HashSet<>();
      responsibleNode.get(k).forEach((ik) -> {
        announceDepartureSet.add( new Item( ik, 
                                            this.items.get(ik).getValue(),
                                            this.items.get(ik).getVersion()));
        });

      p.tell(new Message.AnnounceDeparture(this.key, Collections.unmodifiableSet(announceDepartureSet)), this.getSelf());
    });

    // remove all the peers since the node is no more part of the ring
    this.peers.clear();

    // remove all the data items, since the present node is no more responsible for them
    this.items.clear();
  }

  // receive this message from a node which is leaving the network
  // Consequently I remove it from the list of 
  private void onAnnounceDeparture(Message.AnnounceDeparture msg){
    System.out.println("["+this.getSelf().path().name()+"] [onAnnounceDeparture]");

    // retrive message data
    Integer leavingNodeKey = msg.key;

    // remove the node which is leaving from the ring
    this.peers.remove(leavingNodeKey);

    // add data items of which the present node is responsible for after the departure of the leaving node
    for(Item item : msg.itemSet){
      Item updatedItem = new Item(  item.key,
                                    item.getValue(),
                                    item.getVersion());
      this.items.put(updatedItem.getKey(), updatedItem);  //TODO: riflettere se sono veramente sicuro al 100% che questi item non gli ho mai avuti
    }

  }

  /*END LEAVE*/

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

    // retrive message data and initialize the list of peers
    for (Map.Entry<Integer, ActorRef> pair : msg.activeNodes.entrySet()) {
      this.peers.put(pair.getKey(), pair.getValue());
    }

    // the node add itself to the list of nodes currently active
    // indeed the present node has removed all the elements of this.peers
    // in onRecoveryMsg
    this.peers.put(this.key, this.getSelf()); //TODO: forse nel msg.activeNodes c'è già il this.key

    // ii. the node which is recovering should discard those items that are no longer under its responsability
    for(Integer ik : this.items.keySet()){
      Set<Integer> responsibleNodes = this.getResponsibleNode(ik);
      if(!responsibleNodes.contains(this.key)){
        this.items.remove(ik);
      }
    }

    // the node which is recovering should obtain the items that are now under its responsability
    // this information can be retrived from the clockwise neighbor (which holds all items it needs)
    ActorRef clockwiseNeighbor = this.getClockwiseNeighbor();
    System.out.println("["+this.getSelf().path().name()+"] [onResActiveNodeList_recovery] My clockwise neighbour is: "+clockwiseNeighbor);
    Message.ReqDataItemsResponsibleFor_recovery clockwiseNeighborRequest = new Message.ReqDataItemsResponsibleFor_recovery(this.key, this.items.keySet());
    clockwiseNeighbor.tell(clockwiseNeighborRequest, this.getSelf());
  }

  // this message is received from a node which is recovering that is asking
  // to the present node the items that are now under its responsabilty
  private void onReqDataItemsResponsibleFor_recovery(Message.ReqDataItemsResponsibleFor_recovery msg){
    System.out.println("["+this.getSelf().path().name()+"] [onReqDataItemsResponsibleFor_recovery]");

    // retrive message data
    Integer recoveryNodeKey = msg.key;
    HashSet<Integer> recoveryNodeItemSet = new HashSet<>();
    msg.keyItemSet.forEach(itemKey -> recoveryNodeItemSet.add(itemKey));

    // iterate the data item set to find the data items the joining node is responsible for
    Set<Item> resSet = new HashSet<>();
    for(int k : this.items.keySet()){
      Set<Integer> currentItemResp = this.getResponsibleNode(k);
      if(currentItemResp.contains(recoveryNodeKey) && !recoveryNodeItemSet.contains(k)){
        resSet.add(new Item(k, this.items.get(k).getValue(), this.items.get(k).getVersion()));
      }
    }

    // send the list of data item that the joining node is responsible for
    Message.ResDataItemsResponsibleFor msg_response = new Message.ResDataItemsResponsibleFor(Collections.unmodifiableSet(resSet));
    this.getSender().tell(msg_response, this.getSelf());
  }

  // the node which is currently in crash state receivers the items that are now under its responsability
  // from its clockwise neighbor
  private void onResDataItemsResponsibleFor_recovery(Message.ResDataItemsResponsibleFor msg){
    System.out.println("["+this.getSelf().path().name()+"] [onResDataItemsResponsibleFor_recovery]");

    // retrive message data and add the data items the recovery node is responsible for.
    for(Item item : msg.resSet) { // TODO: chiere a vecchia se si può fare blind o se si deve aggiornare la versione
      this.items.put(item.key, new Item(item.key, item.value, item.version));
    }
    System.out.println("["+this.getSelf().path().name()+"] [onResDataItemsResponsibleFor_recovery] Now I am responsible for the following data items:"+this.items.values());

    // TODO: chiedere a vecchia send read operations

    // exit crash state 
    this.recover();
  }

  /*----------END RECOVERY----------*/

  // TODO: onGet(key)
  // TODO: onUpdate(key, value)



  /*----------GET RESPONSIBLE NODES FOR AN ITEM----------*/

  // given a key, get the set of actors which are responsible
  // for that item according to N
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

  // overwrite getResponsibleNode: in this implementation we set also the set of peer to take into account
  private Set<Integer> getResponsibleNode(int key, TreeSet<Integer> peerRing){
    Set<Integer> responsibleNode = new HashSet<>();
    int n = this.N;

    for (Integer peerKey : peerRing) {
      if(n > 0 && key < peerKey){
        responsibleNode.add(peerKey);
        n--;
      }

      if(n <= 0){
        break;
      }
    }

    if(n > 0){
      for (Integer peerKey : peerRing) {
        if(n > 0){
          responsibleNode.add(peerKey);
          n--;
        } else {
          break;
        }
      }
    }

    return responsibleNode;
  }

  /*----------END GET RESPONSIBLE NODES FOR AN ITEM----------*/

  // get clockwise neighbor
  // this function is called in the context of a join
  // request, so the present node is not part of the network yet (TODO: magari facciamo in modo che invece questo metodo sia generico)
  private ActorRef getClockwiseNeighbor(){

    for(Map.Entry<Integer, ActorRef> entry : this.peers.entrySet()) {
      if(this.key < entry.getKey()){
        return entry.getValue();
      }
    }

    return ((TreeMap<Integer, ActorRef>) this.peers).firstEntry().getValue();
  }

  // get predecessor TODO: riflettere cosa succede quando esiste solo un nodo nel cerchio
  private ActorRef getPredecessor(){
    Integer currentNodeKey = -1;
    Integer prevNodeKey = -1;
    Integer predecessorNodeKey = -1;

    for(Map.Entry<Integer, ActorRef> entry : this.peers.entrySet()) {

      currentNodeKey = entry.getKey();

      if(currentNodeKey == this.key){
        if(prevNodeKey!=-1){
          predecessorNodeKey = prevNodeKey;
        }else{
          predecessorNodeKey = ((TreeMap<Integer, ActorRef>) this.peers).lastEntry().getKey();
        }
        break;
      }

      prevNodeKey = currentNodeKey;
    }

    return this.peers.get(predecessorNodeKey);
  }

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
            Duration.create(this.T, TimeUnit.SECONDS),
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

    Request req = this.requests.get(msg.requestId);

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
        this.requests.remove(msg.requestId);
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
          Duration.create(this.T, TimeUnit.SECONDS),
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
    System.out.println("["+this.getSelf().path().name()+"] [onPrintItemList] Node: " + key +  "  " + this.items);
  }

  /*======================*/
}
