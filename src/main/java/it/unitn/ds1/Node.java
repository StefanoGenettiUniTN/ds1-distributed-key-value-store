package it.unitn.ds1;

import java.util.*;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

import java.util.concurrent.TimeUnit;
import scala.concurrent.duration.Duration;

public class Node extends AbstractActor {
  private final int N;
  private final int R;
  private final int W;
  private final int T;
  private final int MAXRANDOMDELAYTIME = 1; //Maximum delay time in seconds
  private final Random rnd;
  private final Map<Integer, ActorRef> peers;   // peers[K] points to the node in the group with key K
  private final Map<Integer, Item> items;       // the set of data item the node is currently responsible for
  private final Map<Integer, Request> requests; // lists of the requests
  private final Map<Integer, Lock> locks;    // lock mapping used to manage concurrent write

  private int key;  // node key
  private int counterRequest; // Number of request performed from the node as coordinator

  //----FLAGS----
  private Map<Integer, Integer> join_update_item_response_counter;  // the joining node performs read operations to ensure that its items are up to date.
                                                                    // join_update_item_response_counter[item_key] :: the number of version update received about item with key item_key
  
  private boolean flag_ignore_further_read_update;  // flag_ignore_further_read_update == true --> the joining node receive the updated data items from a sufficient number of peers.
                                                    // Hence we can ignore further read update                                                                        
  
  private int leave_response_counter; // the leaving node passes its data items to the nodes that become responsible for them
                                      // after its departure. We wait until each of these nodes send an ACK back. Indeed it is
                                      // possible that some of them are currently in crash state. Consequently, if we send the
                                      // items blindly, we could lose data items or violate replication requirements
  private boolean timeout_AnnounceDeparture_expired;

  private HashMap<Integer, HashSet<Integer>> nodeKeyToResponsibleItem; // data structure used in the context of the leave opreation
  private boolean flag_reqActiveNodeList;
  private boolean timeout_ReqActiveNodeList_expired;
  private boolean flag_reqActiveNodeList_recovery;
  private boolean timeout_ReqActiveNodeList_recover_expired;
  private boolean flag_reqDataItemsResponsibleFor;
  private boolean timeout_ReqDataItemsResponsibleFor_expired;
  private boolean flag_reqDataItemsResponsibleFor_recovery;
  private boolean timeout_ReqDataItemsResponsibleFor_recovery_expired;

  // TODO: noto che alcuni attributi non sono final

  //-------------

  public Node(int n, int r, int w, int t){
    this.N = n;
    this.R = r;
    this.W = w;
    this.T = t;
    this.rnd = new Random();
    this.peers = new TreeMap<>();
    this.items = new HashMap<>();
    this.requests = new HashMap<>();
    this.locks = new HashMap<>();
    this.counterRequest = 0;

    this.join_update_item_response_counter = new HashMap<>();
    this.flag_ignore_further_read_update = false;
    this.leave_response_counter = 0;
    this.timeout_AnnounceDeparture_expired = false;
    this.flag_reqActiveNodeList = false;
    this.timeout_ReqActiveNodeList_expired = false;
    this.flag_reqActiveNodeList_recovery = false;
    this.timeout_ReqActiveNodeList_recover_expired = false;
    this.flag_reqDataItemsResponsibleFor = false;
    this.timeout_ReqDataItemsResponsibleFor_expired = false;
    this.flag_reqDataItemsResponsibleFor_recovery = false;
    this.timeout_ReqDataItemsResponsibleFor_recovery_expired = false;
  }

  @Override
  public void preStart() {
    System.out.println("["+this.getSelf().path().name()+"] [preStart] Node key: "+this.key);
  }

  static public Props props(int n, int r, int w, int t) {
    return Props.create(Node.class, () -> new Node(n, r, w, t));
  }

  // Mapping between the received message types and actor methods
  @Override
  public Receive createReceive() {
    return receiveBuilder()
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
      .match(Message.ReleaseLock.class, this::onReleaseLock)
      .match(Message.ReqDataItemsResponsibleFor.class, this::onReqDataItemsResponsibleFor)
      .match(Message.ReqDataItemsResponsibleFor_recovery.class, this::onReqDataItemsResponsibleFor_recovery)
      .match(Message.ResDataItemsResponsibleFor.class, this::onResDataItemsResponsibleFor)
      .match(Message.JoinReadOperationReq.class, this::onJoinReadOperationReq)
      .match(Message.JoinReadOperationRes.class, this::onJoinReadOperationRes)
      .match(Message.AnnouncePresence.class, this::onAnnouncePresence)
      .match(Message.LeaveMsg.class, this::onLeaveMsg)
      .match(Message.PreLeaveStatusCheck.class, this::onPreLeaveStatusCheck)
      .match(Message.AnnounceDeparture.class, this::onAnnounceDeparture)
      .match(Message.DepartureAck.class, this::onDepartureAck)
      .match(Message.CrashMsg.class, this::onCrashMsg)
      .match(Message.Timeout.class, this::onTimeout)
      .match(Message.Timeout_ReqActiveNodeList.class, this::onTimeout_ReqActiveNodeList)
      .match(Message.Timeout_ReqDataItemsResponsibleFor.class, this::onTimeout_ReqDataItemsResponsibleFor)
      .match(Message.Timeout_JoinReadOperationReq.class, this::onTimeout_JoinReadOperationReq)
      .match(Message.Timeout_AnnounceDeparture.class, this::onTimeout_AnnounceDeparture)
      .match(Message.Timeout_ReqActiveNodeList_recover.class, this::onTimeout_ReqActiveNodeList_recover_ignore)
      .match(Message.Timeout_ReqDataItemsResponsibleFor_recovery.class, this::onTimeout_ReqDataItemsResponsibleFor_recovery_ignore)
      .build();
  }

  // Crash state behaviour
  final AbstractActor.Receive crashed() {
    return receiveBuilder()
      .match(Message.RecoveryMsg.class, this::onRecoveryMsg)
      .match(Message.ResActiveNodeList.class, this::onResActiveNodeList_recovery)
      .match(Message.ResDataItemsResponsibleFor.class, this::onResDataItemsResponsibleFor_recovery)
      .match(Message.Timeout_ReqActiveNodeList_recover.class, this::onTimeout_ReqActiveNodeList_recover)
      .match(Message.Timeout_ReqDataItemsResponsibleFor_recovery.class, this::onTimeout_ReqDataItemsResponsibleFor_recovery)
      .matchAny(msg -> {
        System.out.println(getSelf().path().name() + " ignoring " + msg.getClass().getSimpleName() + " (crashed)");
      })
      .build();
  }

  /*===MESSAGE HANDLERS===*/

  // First node in the network receives this message and
  // initializes the system adding itself to the group
  private void onInit(Message.InitSystem msg){  // TODO: aggiungere InitSystem message alla documentazione
    System.out.println("["+this.getSelf().path().name()+"] [InitSystem] Node key: "+this.key);
    this.key = msg.key;
    this.peers.put(this.key, this.getSelf());
  }

  /*----------JOIN----------*/

  // Node receive the JoinMsg from Main
  private void onJoinMsg(Message.JoinMsg msg){
    System.out.println("["+this.getSelf().path().name()+"] [onJoinMsg]");

    // retrive message data
    this.key = msg.key;
    ActorRef msg_bootstrappingPeer = msg.bootstrappingPeer;

    // ask to the bootstrapping peer the current list of active nodes
    this.flag_reqActiveNodeList = false;
    this.timeout_ReqActiveNodeList_expired = false;
    Message.ReqActiveNodeList reqActiveNodeListMsg = new Message.ReqActiveNodeList();

    // model a random network/processing delay
    try { Thread.sleep(rnd.nextInt(this.MAXRANDOMDELAYTIME*100) * 10); }
    catch (InterruptedException e) { e.printStackTrace(); }
    msg_bootstrappingPeer.tell(reqActiveNodeListMsg, this.getSelf());

    // if the bootstrapping peer does not send a response before the timeout
    // we abort the join process
    getContext().system().scheduler().scheduleOnce(
      Duration.create(this.T, TimeUnit.SECONDS),  // timeout interval
      this.getSelf(),                             // destination actor reference
      new Message.Timeout_ReqActiveNodeList(),    // the message to send,
      getContext().system().dispatcher(),         // system dispatcher
      this.getSelf()                              // source of the message (myself)
    );
  }

  // the bootstrapping node is not sending a response
  // --> abort join operation
  private void onTimeout_ReqActiveNodeList(Message.Timeout_ReqActiveNodeList msg){
    if(this.flag_reqActiveNodeList == false){
      this.timeout_ReqActiveNodeList_expired = true;
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

    // model a random network/processing delay
    try { Thread.sleep(rnd.nextInt(this.MAXRANDOMDELAYTIME*100) * 10); }
    catch (InterruptedException e) { e.printStackTrace(); }

    this.getSender().tell(msg_response, this.getSelf());
  }

  // The node which is joining the network receives the current list
  // of active nodes from the bootstrapping node
  private void onResActiveNodeList(Message.ResActiveNodeList msg){

    // ignore the message if the corresponding timeout has already expired
    if(this.timeout_ReqActiveNodeList_expired){
      return;
    }

    this.flag_reqActiveNodeList = true; // finally the ResActiveNodeList has been received, we can go on with the join operation without aborting
    System.out.println("["+this.getSelf().path().name()+"] [onResActiveNodeList]");

    if(!msg.activeNodes.containsKey(this.key)) {
      // retrive message data and initialize the list of peers
      for (Map.Entry<Integer, ActorRef> pair : msg.activeNodes.entrySet()) {
        this.peers.put(pair.getKey(), pair.getValue());
      }

      // get clocwise neighbor which has to be queried to request data items
      // the joining node is responsible for
      ActorRef clockwiseNeighbor = this.getClockwiseNeighbor();
      System.out.println("[" + this.getSelf().path().name() + "] [onResActiveNodeList] My clockwise neighbour is: " + clockwiseNeighbor);

      // request data items the joining node is responsible for from its clockwise neighbor (which holds all items it needs)
      this.flag_reqDataItemsResponsibleFor = false;
      this.timeout_ReqDataItemsResponsibleFor_expired = false;
      Message.ReqDataItemsResponsibleFor clockwiseNeighborRequest = new Message.ReqDataItemsResponsibleFor(this.key);

      // model a random network/processing delay
      try {
        Thread.sleep(rnd.nextInt(this.MAXRANDOMDELAYTIME * 100) * 10);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      clockwiseNeighbor.tell(clockwiseNeighborRequest, this.getSelf());

      // if the clocwise neighbour peer does not send a response before the timeout
      // we abort the join operation
      getContext().system().scheduler().scheduleOnce(
              Duration.create(this.T, TimeUnit.SECONDS),  // timeout interval
              this.getSelf(),                             // destination actor reference
              new Message.Timeout_ReqDataItemsResponsibleFor(),   // the message to send,
              getContext().system().dispatcher(),         // system dispatcher
              this.getSelf()                              // source of the message (myself)
      );
    } else {
      System.out.println("[" + this.getSelf().path().name() + "] [onResActiveNodeList] Node with the same key already there");
    }
  }

  // the clockwise neighbour node is not sending a response
  // --> abort join operation
  private void onTimeout_ReqDataItemsResponsibleFor(Message.Timeout_ReqDataItemsResponsibleFor msg){
    if(this.flag_reqDataItemsResponsibleFor == false){
      this.timeout_ReqDataItemsResponsibleFor_expired = true;
      System.out.println("["+this.getSelf().path().name()+"] [onTimeout_ReqDataItemsResponsibleFor] ABORT JOIN because no ResDataItemsResponsibleFor has been received before timeout expiration.");
      this.peers.clear(); // TODO: facendo così, nel report si può parlare di optimistic writeahead log? O una roba del genere?
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

    // model a random network/processing delay
    try { Thread.sleep(rnd.nextInt(this.MAXRANDOMDELAYTIME*100) * 10); }
    catch (InterruptedException e) { e.printStackTrace(); }

    this.getSender().tell(msg_response, this.getSelf());
  }

  // the joining node receives the set of data items it is responsible for from its clockwise neighbor
  private void onResDataItemsResponsibleFor(Message.ResDataItemsResponsibleFor msg){

    // ignore the message if the corresponding timeout has already expired
    if(this.timeout_ReqDataItemsResponsibleFor_expired){
      return;
    }

    this.flag_reqDataItemsResponsibleFor = true; // finally the ResDataItemsResponsibleFor has been received, we can go on with the join operation
    System.out.println("["+this.getSelf().path().name()+"] [onResDataItemsResponsibleFor]");

    // retrive message data and add the data items the joining node is responsible for
    for(Item item : msg.resSet) {
      this.items.put(item.key, new Item(item.getKey(), item.getValue(), item.getVersion()));
    }
    System.out.println("["+this.getSelf().path().name()+"] [onResDataItemsResponsibleFor] Now I am responsible for the following data items:"+this.items.values());

    this.join_update_item_response_counter.clear();
    this.flag_ignore_further_read_update = false;
    if(!this.items.isEmpty()){

      //--- perform read operations to ensure that the received items are up to date.
      //--- remark: no read request is sent to the clockwise neighbor which has just sent the current set of items
      //--- for each item we compute the responsible nodes which are the destinations of the update request
      Set<ActorRef> readDestinationNodes = new HashSet<>();

      for(Integer ik : this.items.keySet()){
        for(Integer ik_resp_key : this.getResponsibleNode(ik)){
          if(!this.peers.get(ik_resp_key).equals(this.getSender())){ // we have to ignore the clockwise neighbour which has already sent a response
            this.join_update_item_response_counter.put(ik, 0);
            readDestinationNodes.add(this.peers.get(ik_resp_key));
          }
        }
      }

      // send read operation
      Set<Item> itemSet = new HashSet<>();
      for(Item item : this.items.values()){
        itemSet.add(new Item(item));
      }
      Message.JoinReadOperationReq msg_JoinReadOperationReq = new Message.JoinReadOperationReq(Collections.unmodifiableSet(itemSet)); // TODO: riflettere se mandare a tutti l'intero itemSet oppure solo la parte della quale il dest è responsabile
      for(ActorRef dest : readDestinationNodes){
        // model a random network/processing delay
        try { Thread.sleep(rnd.nextInt(this.MAXRANDOMDELAYTIME*100) * 10); }
        catch (InterruptedException e) { e.printStackTrace(); }
        dest.tell(msg_JoinReadOperationReq, this.getSelf());
      }
      //---
    }

    //System.out.println("join_update_item_response_counter = "+join_update_item_response_counter);

    // in the case this.join_update_item_response_counter is empty then it is not necessary to perform any read operation
    // and the node can immediately announce its presence to every node in the system
    if(this.join_update_item_response_counter.isEmpty()){
      
      // the node add itself to the list of nodes currently active
      this.peers.put(this.key, this.getSelf());

      // the node can finally announce its presence to every node in the system
      Set<Integer> announcePresenceKeyItemSet = new HashSet<>(this.items.keySet());
      Message.AnnouncePresence announcePresence = new Message.AnnouncePresence(this.key, Collections.unmodifiableSet(announcePresenceKeyItemSet));
      this.peers.forEach((k, p) -> {
        if(!p.equals(this.getSelf())){
          // model a random network/processing delay
          try { Thread.sleep(rnd.nextInt(this.MAXRANDOMDELAYTIME*100) * 10); }
          catch (InterruptedException e) { e.printStackTrace(); }
          p.tell(announcePresence, this.getSelf());
        }
      });
    }else{
      // if at least one of the peers which should send a JoinReadOperationRes message, does not reply within
      // the timeout interval, we abort the join operation
      getContext().system().scheduler().scheduleOnce(
        Duration.create(this.T, TimeUnit.SECONDS),  // timeout interval
        this.getSelf(),                             // destination actor reference
        new Message.Timeout_JoinReadOperationReq(), // the message to send,
        getContext().system().dispatcher(),         // system dispatcher
        this.getSelf()                              // source of the message (myself)
      );
    }
  }

  // not enough peers which should have sent a JoinReadOperationRes message, replied
  // within the timeout interval, we abort the join operation and rollback the state.
  // More in depth, for each item requested, at least N-1 peers should have sent a 
  // response.
  private void onTimeout_JoinReadOperationReq(Message.Timeout_JoinReadOperationReq msg){
    for(Map.Entry<Integer, Integer> entry : this.join_update_item_response_counter.entrySet()){
      if(entry.getValue() < this.R - 1){
        System.out.println("["+this.getSelf().path().name()+"] [onTimeout_JoinReadOperationReq] ABORT JOIN because not all the expected nodes have sent a JoinReadOperationRes message");
        this.flag_ignore_further_read_update = true;
        this.peers.clear();
        this.items.clear();
      }
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
      if(this.items.containsKey(item.getKey())){  // the current node has the item
        Item updatedItem = new Item(  item.key,
                                      this.items.get(item.getKey()).getValue(),
                                      this.items.get(item.getKey()).getVersion());
        updatedItems.add(updatedItem);
      }
    }

    // send the update version of the requested items
    // to the node which is joining the network
    Message.JoinReadOperationRes joinReadOperationResponse = new Message.JoinReadOperationRes(Collections.unmodifiableSet(updatedItems));

    // model a random network/processing delay
    try { Thread.sleep(rnd.nextInt(this.MAXRANDOMDELAYTIME*100) * 10); }
    catch (InterruptedException e) { e.printStackTrace(); }
    this.getSender().tell(joinReadOperationResponse, this.getSelf());
  }

  // The joining node is receiving the updated version of its items from the other peers.
  // As soon as the joining node receives the JoinReadOperationRes from all the requested
  // nodes, the node can finally announce its presence to every node in the system and start
  // serving requests coming from clients.
  private void onJoinReadOperationRes(Message.JoinReadOperationRes msg){
    
    // the timeout is expired
    if(this.flag_ignore_further_read_update){
      return;
    }

    System.out.println("["+this.getSelf().path().name()+"] [onJoinReadOperationRes]");

    // retrive message data and update the items in this.items accordingly
    for(Item item : msg.responseItemSet){

      // increase the counter of reply about this item that the present node
      // has received
      this.join_update_item_response_counter.put(item.getKey(), this.join_update_item_response_counter.get(item.getKey())+1);

      if(this.items.get(item.getKey()).getVersion() < item.getVersion()){
        Item updatedItem = new Item(  item.key,
                                      item.getValue(),
                                      item.getVersion());
        this.items.put(updatedItem.getKey(), updatedItem);
      }
    }

    // if this for iteration completes without executing the return
    // then all the requested nodes have sent the updated version of the items.
    // Finally, the present joining node can announce its presence to every
    // node in the system and start serving requests coming from clients.
    for(Map.Entry<Integer, Integer> entry : this.join_update_item_response_counter.entrySet()){
      if(entry.getValue() < this.R - 1){
        return;
      }
    }

    // ignore further JoinReadOperationRes
    this.flag_ignore_further_read_update = true;

    // the node add itself to the list of nodes currently active
    this.peers.put(this.key, this.getSelf());

    // the node can finally announce its presence to every node in the system
    Set<Integer> announcePresenceKeyItemSet = new HashSet<>(this.items.keySet());
    Message.AnnouncePresence announcePresence = new Message.AnnouncePresence(this.key, Collections.unmodifiableSet(announcePresenceKeyItemSet));
    this.peers.forEach((k, p) -> {
      if(!p.equals(this.getSelf())){
        // model a random network/processing delay
        try { Thread.sleep(rnd.nextInt(this.MAXRANDOMDELAYTIME*100) * 10); }
        catch (InterruptedException e) { e.printStackTrace(); }
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

    // for each item in keyItemSet which is also in this.items, check if
    // the present node is still responsible for. Consequently, remove the data items
    // the present node is no longer responsible for. 
    for(Integer itemKey : msg.keyItemSet){
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
    
    this.nodeKeyToResponsibleItem = new HashMap<>();  // nodeKeyToResponsibleItem[k] :: list of item keys the node k is
                                                      // responsible for after the departure of the present node
    this.peers.keySet().forEach((peerKey) -> {this.nodeKeyToResponsibleItem.put(peerKey, new HashSet<>());});  

    // fill the list of responsible node (nodeKeyToResponsibleItem) for each item in this.items
    for(Integer itemKey : this.items.keySet()){
      Set<Integer> responsibleNodeKeys = this.getResponsibleNode(itemKey);
      responsibleNodeKeys.forEach((respNodeKey) -> {
        if(respNodeKey != this.key){
          HashSet<Integer> respNodeSet = this.nodeKeyToResponsibleItem.get(respNodeKey);
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
        if(this.nodeKeyToResponsibleItem.get(respNodeKey).contains(itemKey)){ // in this case the node was already resoponsible for the item even before the departure of the present node
          this.nodeKeyToResponsibleItem.get(respNodeKey).remove(itemKey);
        }else{
          this.nodeKeyToResponsibleItem.get(respNodeKey).add(itemKey);
        }
      });
    }

    // before completing the execution of the leave, we need to be sure that the peers
    // which are going to become responsible of some data items after the departure of the
    // present node, are not crashed. To avoid this we send an PreLeaveStatusCheck message.
    this.leave_response_counter = 0;
    this.timeout_AnnounceDeparture_expired = false;
    for (Integer peerKey : this.peers.keySet()) {
      if(!this.nodeKeyToResponsibleItem.get(peerKey).isEmpty()){
        // set leave response counter, i.e. the number of ACK that we have to wait before completing the leave operation
        this.leave_response_counter++;

        // send PreLeaveStatusCheck message
        this.peers.get(peerKey).tell(new Message.PreLeaveStatusCheck(), this.getSelf());
      }
    }

    if(this.leave_response_counter == 0){ // all the expected peers have sent the acknowledgment --> complete leave operation

      // the node announces to every other node that it is leaving
      // the node passes its data items to the nodes that become responsible for them after its departure
      this.peers.forEach((k, p) -> {

        // prepare announce departure message
        Set<Item> announceDepartureSet = new HashSet<>();
        this.nodeKeyToResponsibleItem.get(k).forEach((ik) -> {
          announceDepartureSet.add( new Item( ik, 
                                              this.items.get(ik).getValue(),
                                              this.items.get(ik).getVersion()));
          });
        
        // model a random network/processing delay
        try { Thread.sleep(rnd.nextInt(this.MAXRANDOMDELAYTIME*100) * 10); }
        catch (InterruptedException e) { e.printStackTrace(); }
        p.tell(new Message.AnnounceDeparture(this.key, Collections.unmodifiableSet(announceDepartureSet)), this.getSelf());
      });

      // remove all the peers since the node is no more part of the ring
      this.peers.clear();

      // remove all the data items, since the present node is no more responsible for them
      this.items.clear();

    }else{  // set timeout
      // if the peers which should become responsible of some of the data items of the present node
      // does not send an ack withing the timeout interval,  we abort the leave operation
      getContext().system().scheduler().scheduleOnce(
        Duration.create(this.T, TimeUnit.SECONDS),  // timeout interval
        this.getSelf(),                             // destination actor reference
        new Message.Timeout_AnnounceDeparture(),    // the message to send,
        getContext().system().dispatcher(),         // system dispatcher
        this.getSelf()                              // source of the message (myself)
      );
    }
  }

  // the leaving node has sent this message to the present node in order to be sure that the present node is
  // currently available (not crashed)
  private void onPreLeaveStatusCheck(Message.PreLeaveStatusCheck msg){
    System.out.println("["+this.getSelf().path().name()+"] [onPreLeaveStatusCheck]");
    this.getSender().tell(new Message.DepartureAck(), this.getSelf());
  }

  // the leaving node is receing this ack from one of its peers which is going to become responsible of some
  // of the items currently stored by the leaving node after the departure of this last
  private void onDepartureAck(Message.DepartureAck msg){

    // ignore the message if the corresponding timeout has already expired
    if(this.timeout_AnnounceDeparture_expired){
      return;
    }

    System.out.println("["+this.getSelf().path().name()+"] [onDepartureAck]");
    this.leave_response_counter--;

    if(this.leave_response_counter == 0){ // all the expected peers have sent the acknowledgment --> complete leave operation

      // the node announces to every other node that it is leaving
      // the node passes its data items to the nodes that become responsible for them after its departure
      this.peers.forEach((k, p) -> {

        // prepare announce departure message
        Set<Item> announceDepartureSet = new HashSet<>();
        this.nodeKeyToResponsibleItem.get(k).forEach((ik) -> {
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

      // clear responsible node
      this.nodeKeyToResponsibleItem.clear();
    }

  }

  // this timeout message is sent when the leaving node does not receive an acknowledgement from one or more
  // of the nodes that should become responsible of its data items after the departure.
  // As a consequence we need to abort the leave operation and rollback the state.
  private void onTimeout_AnnounceDeparture(Message.Timeout_AnnounceDeparture msg){
    if(this.leave_response_counter > 0){  // there are still nodes which have not sent the ACK yet
      this.timeout_AnnounceDeparture_expired = true;
      System.out.println("["+this.getSelf().path().name()+"] [onTimeout_AnnounceDeparture] ABORT LEAVE because not all the ACK have been receiver from the target nodes.");
      this.peers.put(this.key, this.getSelf());
    }
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
    // model a random network/processing delay
    this.flag_reqActiveNodeList_recovery = false;
    this.timeout_ReqActiveNodeList_recover_expired = false;
    try { Thread.sleep(rnd.nextInt(this.MAXRANDOMDELAYTIME*100) * 10); }
    catch (InterruptedException e) { e.printStackTrace(); }
    bootstrappingPeer.tell(new Message.ReqActiveNodeList(), this.getSelf());

    // if the bootstrapping peer does not send a response before the timeout
    // we abort the recovery process
    getContext().system().scheduler().scheduleOnce(
      Duration.create(this.T, TimeUnit.SECONDS),  // timeout interval
      this.getSelf(),                             // destination actor reference
      new Message.Timeout_ReqActiveNodeList_recover(),    // the message to send,
      getContext().system().dispatcher(),         // system dispatcher
      this.getSelf()                              // source of the message (myself)
    );
  }

  // the bootstrapping node is not sending a response
  // --> abort recovery operation
  private void onTimeout_ReqActiveNodeList_recover(Message.Timeout_ReqActiveNodeList_recover msg){
    if(this.flag_reqActiveNodeList_recovery == false){
      this.timeout_ReqActiveNodeList_recover_expired = true;
      System.out.println("["+this.getSelf().path().name()+"] [onTimeout_ReqActiveNodeList_recover] ABORT RECOVERY because no ResActiveNodeList has been received before timeout expiration.");
    }
  }

  // if the node is not crash and receives a Timeout_ReqActiveNodeList_recover message it has to ignore it
  private void onTimeout_ReqActiveNodeList_recover_ignore(Message.Timeout_ReqActiveNodeList_recover msg){}

  // The bootstrapping node send the current list of the active nodes
  // to the node which is recovering from crash
  private void onResActiveNodeList_recovery(Message.ResActiveNodeList msg){

    // ignore the message if the corresponding timeout has already expired
    if(this.timeout_ReqActiveNodeList_recover_expired){
      return;
    }

    this.flag_reqActiveNodeList_recovery = true; // finally the ResActiveNodeList has been received, we can go on with the recovery operation without aborting
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
    Set<Item> backup = new HashSet<>(); // we make a backup of those items that are no longer under the responsability of the present node. In this way, in the case of a timeout we can recover these items before aborting the recovery operation.
    for(Integer ik : this.items.keySet()){
      Set<Integer> responsibleNodes = this.getResponsibleNode(ik);
      if(!responsibleNodes.contains(this.key)){
        backup.add(new Item(ik, this.items.get(ik).getValue(), this.items.get(ik).getVersion()));
      }
    }
    for(Item backup_item : backup){
      this.items.remove(backup_item.getKey());
    }

    // the node which is recovering should obtain the items that are now under its responsability
    // this information can be retrived from the clockwise neighbor (which holds all items it needs)
    ActorRef clockwiseNeighbor = this.getClockwiseNeighbor();
    System.out.println("["+this.getSelf().path().name()+"] [onResActiveNodeList_recovery] My clockwise neighbour is: "+clockwiseNeighbor);
    this.flag_reqDataItemsResponsibleFor_recovery = false;
    this.timeout_ReqDataItemsResponsibleFor_recovery_expired = false;
    Message.ReqDataItemsResponsibleFor_recovery clockwiseNeighborRequest = new Message.ReqDataItemsResponsibleFor_recovery(this.key, this.items.keySet());
    clockwiseNeighbor.tell(clockwiseNeighborRequest, this.getSelf());

    // if the clockwise neighbour peer does not send a response before the timeout
    // we abort the recovery process
    getContext().system().scheduler().scheduleOnce(
      Duration.create(this.T, TimeUnit.SECONDS),  // timeout interval
      this.getSelf(),                             // destination actor reference
      new Message.Timeout_ReqDataItemsResponsibleFor_recovery(Collections.unmodifiableSet(backup)),    // the message to send,
      getContext().system().dispatcher(),         // system dispatcher
      this.getSelf()                              // source of the message (myself)
    );

  }

  // the clockwise neighbour node is not sending a response
  // --> abort recovery operation
  private void onTimeout_ReqDataItemsResponsibleFor_recovery(Message.Timeout_ReqDataItemsResponsibleFor_recovery msg){
    if(this.flag_reqDataItemsResponsibleFor_recovery == false){
      this.timeout_ReqDataItemsResponsibleFor_recovery_expired = true;
      System.out.println("["+this.getSelf().path().name()+"] [onTimeout_ReqDataItemsResponsibleFor_recovery] ABORT RECOVERY because no ReqDataItemsResponsibleFor_recovery has been received before timeout expiration.");
      this.peers.clear();

      // restore the data items that have been deleted by the present node during the recovery process
      // because they would have been no more under its responsability
      for(Item it : msg.backupItemSet){
        this.items.put(it.getKey(), new Item(it.getKey(), it.getValue(), it.getVersion()));
      }
    }
  }

  // if the node is not crash and receives a Timeout_ReqDataItemsResponsibleFor_recovery message it has to ignore it
  private void onTimeout_ReqDataItemsResponsibleFor_recovery_ignore(Message.Timeout_ReqDataItemsResponsibleFor_recovery msg){}

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

  // iii. obtain the items that are now under its responsability
  // the node which is currently in crash state receivers the items that are now under its responsability
  // from its clockwise neighbor
  private void onResDataItemsResponsibleFor_recovery(Message.ResDataItemsResponsibleFor msg){

    // ignore the message if the corresponding timeout has already expired
    if(this.timeout_ReqDataItemsResponsibleFor_recovery_expired){
      return;
    }

    this.flag_reqDataItemsResponsibleFor_recovery = true; // finally the ResDataItemsResponsibleFor has been received, we can go on with the recovery operation without aborting
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

  /*----------GET RESPONSIBLE NODES FOR AN ITEM----------*/

  // given a key, get the set of actors which are responsible
  // for that item according to N
  private Set<Integer> getResponsibleNode(int key){
    Set<Integer> responsibleNode = new HashSet<>();
    int n = this.N;

    //For each node, check if it is responsible for the item based on the key
    for (Map.Entry<Integer, ActorRef> entry : peers.entrySet()) {
      //A node is responsible for an item if N > 0 and the key of the item is lower than the key of the nodes
      if(n > 0 && key < entry.getKey()){
        //If the condition is satisfied we add the key of the node and decrease N, which is the number of nodes responsible for an item
        responsibleNode.add(entry.getKey());
        n--;
      }

      //If we get enough nodes, we can stop locking at the nodes
      if(n <= 0){
        break;
      }
    }

    //If N is greater than 0, this means that we have not enough nodes and we will made the first nodes responsible for the item
    //i. this happens when the key of the item is greated of all the item of the nodes or when
    //ii. we have not enough nodes with a greater key than the item key
    //In both cases the items will be assigned to the first N available nodes
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
  // request, so the present node is not part of the network yet (TODO: magari facciamo in modo che invece questo metodo sia generico. TODO: riflettere su cosa succede quando viene chiamato questo metodo in recovery, mi sono accordo che in quel caso this.key è parte del ring.)
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

  //The Get operation is performed in this way:
  //a. the coordinator receive the request from the client and contact the N responsible nodes for reading the item
  //b. the responsible nodes return the item they own
  //c. coordinator waits for R replies and then return to the client the last version of the item requests


  //a. the coordinator receive the request from the client and contact the N responsible nodes for reading the item

  // This is the request from the client to the node coordinator for the operation. This operation will be executed in this order:
  // i. set a new counterrequest for the request
  // ii. get the responsible nodes for the item provided
  // iii. if the coordinator is one of the responsible nodes it will read it
  // iV. check if the quorum R is reached
  // V. if it is not reached ask to the other responsible nodes
  // VI. if it is reached return the response and delete the request
  private void onGetRequest(Message.GetRequest msg){
    Item item =  new Item(msg.item);
    System.out.println("["+this.getSelf().path().name()+"] [onGet] Coordinator");

    // i. set a new counterrequest for the request
    counterRequest++;
    Request req = new Request(this.getSender(), new Item(key, ""), Type.GET);
    this.requests.put(counterRequest, req);

    // ii. get the responsible nodes for the item provided
    Set<Integer> respNodes = getResponsibleNode(item.getKey());

    // iii. if the coordinator is one of the responsible nodes it will read it
    if(respNodes.contains(this.key)){
      //Check if no lock is present on the item
      // if no lock is set, the coordinator can read the item
      // if it is set, the coordinator cannot read the item since a write operation is ongoing and version problems could arise
      if(this.items.containsKey(item.getKey()) && this.locks.get(item.getKey()) == null) {
        req.setCounter(req.getCounter() + 1);
        item.setVersion(msg.item.getVersion());
        item.setValue(msg.item.getValue());
      }
    }

    int nR = req.getCounter();

    // iV. check if the quorum R is reached
    if(nR < this.R) {
      //Send a message to all the responsible nodes
      for (int node : respNodes) {
        //Avoid to send the message to itself (message directly read before)
        if(node != this.key) {
          // model a random network/processing delay
          try { Thread.sleep(rnd.nextInt(this.MAXRANDOMDELAYTIME*100) * 10); }
          catch (InterruptedException e) { e.printStackTrace(); }
          (peers.get(node)).tell(new Message.Read(counterRequest, item), this.getSelf());
        }
      }

      //Set a timeout that will expire if not enough (R) replies arrives
      getContext().system().scheduler().scheduleOnce(
              Duration.create(Main.T, TimeUnit.SECONDS),
              this.getSelf(),
              new Message.Timeout(null, counterRequest, item.getKey()), // the message to send,
              getContext().system().dispatcher(), this.getSelf()
      );
    } else if (nR == this.R){ // this should happen only if R is set to 1
      System.out.println("["+this.getSelf().path().name()+"] [onDirectReadItemInformation] Owner");
      //Remove the request from the array
      this.requests.remove(counterRequest);
      // model a random network/processing delay
      try { Thread.sleep(rnd.nextInt(this.MAXRANDOMDELAYTIME*100) * 10); }
      catch (InterruptedException e) { e.printStackTrace(); }
      //Return the result of the read operation
      req.getClient().tell(new ClientMessage.GetResult(Result.SUCCESS, item), ActorRef.noSender());
    }
  }


  //b. the responsible nodes return the item they own

  //Manage the request to read an item
  // i. firstly check if a lock is not set (not writing operation ongoing) and if the item is not null (item stored)
  // ii. return the item stored
  private void onRead(Message.Read msg){
    Item item = this.items.get(msg.item.getKey());
    Lock checkLock = this.locks.get(msg.item.getKey());
    // i. firstly check if a lock is not set (not writing operation ongoing) and if the item is not null (item stored)
    if(checkLock == null && item != null) {
      System.out.println("[" + this.getSelf().path().name() + "] [onRead] Owner: " + key + " ITEM: " + item);
      // model a random network/processing delay
      try { Thread.sleep(rnd.nextInt(this.MAXRANDOMDELAYTIME*100) * 10); }
      catch (InterruptedException e) { e.printStackTrace(); }
      // ii. return the item stored
      this.getSender().tell(new Message.ReadItemInformation(msg.requestId, item), ActorRef.noSender());
    }
  }

  //c. coordinator waits for R replies and then return to the client the last version of the item requests

  //Coordinator has to manage the replies for get operation request in this way
  // i. check if request is set (if the timeout has not expired)
  // ii. Increase the number of replies received
  // iii. Check if the version sent is newer than the one actually stored
  // iV. if yes, update the value and the version
  // V. check if the quorum R is reached
  // Vi. if it is reached remove the request and return the updated item
  private void onReadItemInformation(Message.ReadItemInformation msg){
    System.out.println("["+this.getSelf().path().name()+"] [onReadItemInformation] Owner");

    Request req = this.requests.get(msg.requestId);

    // i. check if request is set (if the timeout has not expired)
    if(req != null) {
      // ii. Increase the number of replies received
      req.setCounter(req.getCounter() + 1);
      int nR = req.getCounter();

      Item item = req.getItem();
      // iii. Check if the version sent is newer than the one actually stored
      if(msg.item.getVersion() > item.getVersion()){
        // iV. if yes, update the value and the version
        item.setVersion(msg.item.getVersion());
        item.setValue(msg.item.getValue());
      }

      // V. check if the quorum R is reached
      if(nR == this.R) {
        // Vi. if it is reached remove the request and return the updated item
        this.requests.remove(msg.requestId);

        // model a random network/processing delay
        try { Thread.sleep(rnd.nextInt(this.MAXRANDOMDELAYTIME*100) * 10); }
        catch (InterruptedException e) { e.printStackTrace(); }

        req.getClient().tell(new ClientMessage.GetResult(Result.SUCCESS, msg.item), ActorRef.noSender());
      }
    }
  }

  /*----------END GET----------*/

  //Manage the release of the lock; check if the lock is the one that corresponds to the one requested
  // if the condition is satisfied, remove the lock
  private void onReleaseLock(Message.ReleaseLock msg){
    Lock lock = this.locks.get(msg.itemId);
    if(lock != null && lock.equals(msg.lock)){
      this.locks.remove(msg.itemId);
    }
  }

  /*----------TIMEOUT----------*/

  //Manage the timeout for the read and write operation in this way
  // i. Check if the request is set, if it is set
  // ii. Check if the operation requested is a read or write
  // iii. if the operation is a get, simply return the errot get response
  // iV. if it is a write operation, do:
  // V. get the list of the responsible nodes
  // Vi. Check if the coordinator is one of the responsible nodes
  // Vii. if the coordinator is one of the responsible nodes and the lock corresponds to the one requested, remove it
  // Viii. Ask to the other nodes to release the requested lock
  // iX. return the error write operation response
  private void onTimeout(Message.Timeout msg){
    // i. Check if the request is set, if it is set
    if(this.requests.containsKey(msg.requestId) == true){
      Request req = this.requests.remove(msg.requestId);
      // ii. Check if the operation requested is a read or write
      if(req.getType() == Type.GET) {
        // model a random network/processing delay
        try { Thread.sleep(rnd.nextInt(this.MAXRANDOMDELAYTIME*100) * 10); }
        catch (InterruptedException e) { e.printStackTrace(); }
        System.out.println("["+this.getSelf().path().name()+"] [onTimeout_ReadOperation] ABORT GET REQUEST");
        // iii. if the operation is a get, simply return the errot get response
        req.getClient().tell(new ClientMessage.GetResult(Result.ERROR, null), ActorRef.noSender());
      } else {
        // iV. if it is a write operation, do:

        // V. get the list of the responsible nodes
        Set<Integer> nodes = getResponsibleNode(msg.itemId);

        // Vi. Check if the coordinator is one of the responsible nodes
        if(nodes.contains(this.key)){
          // Vii. if the coordinator is one of the responsible nodes and the lock corresponds to the one requested, remove it
          Lock checkLock = this.locks.get(msg.itemId);
          if(checkLock != null && checkLock == msg.lock) {
            this.locks.remove(msg.itemId);
          }
        }

        // Viii. Ask to the other nodes to release the requested lock
        for (int node : nodes) {
          if (node != this.key) {
            // model a random network/processing delay
            try { Thread.sleep(rnd.nextInt(this.MAXRANDOMDELAYTIME*100) * 10); }
            catch (InterruptedException e) { e.printStackTrace(); }

            (peers.get(node)).tell(new Message.ReleaseLock(msg.lock, msg.itemId), this.getSelf());
          }
        }

        // model a random network/processing delay
        try { Thread.sleep(rnd.nextInt(this.MAXRANDOMDELAYTIME*100) * 10); }
        catch (InterruptedException e) { e.printStackTrace(); }
        System.out.println("["+this.getSelf().path().name()+"] [onTimeout_WriteOperation] ABORT UPDATE REQUEST");
        // iX. return the error write operation response
        req.getClient().tell(new ClientMessage.UpdateResult(Result.ERROR, null), ActorRef.noSender());
      }
    }
  }

  /*----------END TIMEOUT----------*/

  /*----------UPDATE----------*/

  //Update protocols works in this way:
  // a. coordinator receive the request from the client and request to the N responsible nodes the actual last version of the item
  // b. the responsible nodes return the last version of the item they own
  // c. coordinator waits W replies, send the confirmation message to the client and request to the responsible nodes to update the item
  // d. the responsible nodes update the item


  // a. coordinator receive the request from the client and request to the N responsible nodes the actual last version of the item
  
  // Coordinator receive the update request from the client and perform the following protocol:
  // i: check if the item is null (not already set), or if the lock is null or the lock is the same of the request it is performing; if one of the conditions matches:
  // ii. set the new request and update the lock requested with the request counter
  // iii. get the responsible nodes for the item
  // iV. check if the coordinator is responsible for the item
  // V. if the coordinator is responsible, get the lock, update the number of writing response and if the item is not null update the version
  // Vi. check if the W quorum is reached
  // Vii. if the quorum W is not reached, requested to the other responsible nodes
  // Viii. If the quorum W is reached: increase the version, release the lock, remove the request, send the confirmation to the client and write the item
  private void onUpdateRequest(Message.UpdateRequest msg){
    Item item = new Item(msg.item);
    String clientName = msg.clientName;
    System.out.println("["+this.getSelf().path().name()+"] [onUpdate] Coordinator");

    Lock lock = this.locks.get(item.getKey());
    Item itemNode = this.items.get(item.getKey());
    Lock newLock = new Lock(clientName, this.key, 0);

    // i: check if the item is null (not already set), or if the lock is null or the lock is the same of the request it is performing; if one of the conditions matches:
    if(itemNode == null || lock == null || lock.equals(newLock)) {
      // ii. set the new request and update the lock requested with the request counter
      counterRequest++;
      newLock.setCounterRequest(counterRequest);
      Request req = new Request(this.getSender(), item, Type.UPDATE);
      this.requests.put(counterRequest, req);

      // iii. get the responsible nodes for the item
      Set<Integer> respNodes = getResponsibleNode(item.getKey());

      // iV. check if the coordinator is responsible for the item
      if (respNodes.contains(this.key)) {
        // SEMMAI VERIFICARE QUI DI AVERE IL LOCK
        // V. if the coordinator is responsible, get the lock, update the number of writing response and if the item is not null update the version
        // Set the lock
        this.locks.put(item.getKey(), newLock);
        System.out.println("["+this.getSelf().path().name()+"] [onUpdate] Coordinator 1 item "+ item.getKey() + " lock-coordinator: " + this.key);
        //Increase the responses relative to the update request
        req.setCounter(req.getCounter() + 1);

        //Check if the item is already stored in the node
        if (this.items.get(item.getKey()) != null) {
          //Update the version of the item with the one owned by the coordinator
          item.setVersion((this.items.get(item.getKey())).getVersion());
        }
      }

      //Get the number of write replies
      int nW = req.getCounter();

      // Vi. check if the W quorum is reached
      if (nW < this.W) {
        // Vii. if the quorum W is not reached, requested to the other responsible nodes
        for (int node : respNodes) {
          //Avoid to sent a useless message to itself
          if (node != this.key) {
            // model a random network/processing delay
            try { Thread.sleep(rnd.nextInt(this.MAXRANDOMDELAYTIME*100) * 10); }
            catch (InterruptedException e) { e.printStackTrace(); }

            (peers.get(node)).tell(new Message.Version(newLock, counterRequest, item), this.getSelf());
          }
        }

        //Set a timeout that will expires if not enough W replies arrives in time
        getContext().system().scheduler().scheduleOnce(
                Duration.create(Main.T, TimeUnit.SECONDS),
                this.getSelf(),
                new Message.Timeout(newLock, counterRequest, item.getKey()), // the message to send,
                getContext().system().dispatcher(), this.getSelf()
        );
      } else if (nW == this.W) { // this should be only if W is 1
        // Viii. If the quorum W is reached: increase the version, release the lock, remove the request, send the confirmation to the client and write the item
        //Update the version to a new one
        item.setVersion(item.getVersion() + 1);
        Lock checkLock = this.locks.get(item.getKey());
        //Check if the lock is the same you request
        if(checkLock != null && checkLock.equals(newLock)) {
          //release the lock
          this.locks.remove(item.getKey());
        }
        //Remove the requests since it is satisfied (otherwise the timeout will expire
        this.requests.remove(counterRequest);
        // model a random network/processing delay
        try { Thread.sleep(rnd.nextInt(this.MAXRANDOMDELAYTIME*100) * 10); }
        catch (InterruptedException e) { e.printStackTrace(); }
        //Send the result of the operation to the client
        req.getClient().tell(new ClientMessage.UpdateResult(Result.SUCCESS, item), ActorRef.noSender());
        //Update the result
        this.items.put(item.getKey(), item);
        System.out.println("[" + this.getSelf().path().name() + "] [onDirectWriteInformation] Owner: lock: item " + item.getKey() +  " -> lock " + lock + " -> coordinator " + this.key);
      }
    } else {
      // CHIEDERE A STEFANO: SERVE OPPURE E' DANNOSO PERCHE' ANCHE SE IL COORDINATORE HA IL LOCK POTREI CHIEDERE AGLI ALTRI?
      //If the coordinator is not able to get the lock return the wrong result
      System.out.println("["+this.getSelf().path().name()+"] [onUpdate] Coordinator: Locked  item " + item.getKey() + " -> lock " + lock + " -> coordinator " + this.key);
      // model a random network/processing delay
      try { Thread.sleep(rnd.nextInt(this.MAXRANDOMDELAYTIME*100) * 10); }
      catch (InterruptedException e) { e.printStackTrace(); }
      this.getSender().tell(new ClientMessage.UpdateResult(Result.ERROR, null), ActorRef.noSender());
    }
  }

  // b. the responsible nodes return the last version of the item they own

  //Manage the response to the request of the version from the coordinator
  // i. check if no lock (or same lock) is set (if the condition is not matched no response)
  // ii. get the lock
  // iii. check if item is not null and if so get the version of the item stored
  // iV. return the item with the actual version stored
  private void onVersion(Message.Version msg){
    Item item = new Item(msg.item);
    Lock lock = this.locks.get(item.getKey());
    Lock msg_lock = msg.lock;
    // i. check if no lock (or same lock) is set (if the condition is not matched no response)
    if(lock == null || lock.equals(msg_lock)) {
      System.out.println("[" + this.getSelf().path().name() + "] [onVersion] Owner lock: item key " + item.getKey() +  " -> lock " + lock + " -> coordinator: " + msg_lock + " (on node): -> " + this.key);
      Item itemNode = this.items.get(item.getKey());
      // ii. get the lock
      this.locks.put(item.key, msg_lock);

      // iii. check if item is not null and if so get the version of the item stored
      if (itemNode != null) {
        item.setVersion(itemNode.getVersion());
      }

      // model a random network/processing delay
      try { Thread.sleep(rnd.nextInt(this.MAXRANDOMDELAYTIME*100) * 10); }
      catch (InterruptedException e) { e.printStackTrace(); }

      // iV. return the item with the actual version stored
      this.getSender().tell(new Message.UpdateVersion(msg_lock, msg.requestId, item), this.getSelf());
    } else {
      //No response if the lock is not available
      System.out.println("[" + this.getSelf().path().name() + "] [onVersion] Owner: It's locked: item " + item.getKey() +  " -> lock " + lock +  " -> coordinator " + msg_lock + " (on node) " + this.key);
    }
  }

  // c. coordinator waits W replies, send the confirmation message to the client and request to the responsible nodes to update the item

  //Coordinator has to manage the response messages oof the version with this protocol:
  // i. check if the request is yet there (no timeout expired)
  // ii. Update the number of the responses relative to the write requested
  // iii. Check if the quorum has been reached
  // iV. if not, check only if the version of the item received is newer than the one stored, if so update the version of stored item
  // V. if quorum is reached perform the following operation
  // Vi. update the version of the item to a new one (the one stored is the last already there)
  // Vii. remove the request (otherwise the timeout will expire)
  // Viii. return the response to the client (with the new item updated)
  // iV. Send the item to update to all the N nodes
  private void onUpdateVersion(Message.UpdateVersion msg){
    Item item = new Item(msg.item);
    System.out.println("["+this.getSelf().path().name()+"] [onUpdateVersion] Coordinator");

    Request req = this.requests.get(msg.requestId);

    // i. check if the request is yet there (no timeout expired)
    if(req != null) {
      // ii. Update the number of the responses relative to the write requested
      req.setCounter(req.getCounter() + 1);
      Item itemReq = req.getItem();
      int nW = req.getCounter();

      // iii. Check if the quorum has been reached
      if(nW < this.W){
        // iV. if not, check only if the version of the item received is newer than the one stored, if so update the version of stored item
        if(msg.item.getVersion() > itemReq.getVersion()){
          itemReq.setVersion(msg.item.getVersion());
        }
      } else if(nW == this.W) {
        // v. if quorum is reached perform the following operation
        // Vi. update the version of the item to a new one (the one stored is the last already there)
        itemReq.setVersion(itemReq.getVersion() + 1);

        // Vii. remove the request (otherwise the timeout will expire)
        this.requests.remove(msg.requestId);

        // model a random network/processing delay
        try { Thread.sleep(rnd.nextInt(this.MAXRANDOMDELAYTIME*100) * 10); }
        catch (InterruptedException e) { e.printStackTrace(); }

        // Viii. return the response to the client (with the new item updated)
        req.getClient().tell(new ClientMessage.UpdateResult(Result.SUCCESS, itemReq), ActorRef.noSender());

        // iV. Send the item to update to all the N nodes
        for (int node : getResponsibleNode(item.getKey())) {
          // model a random network/processing delay
          try { Thread.sleep(rnd.nextInt(this.MAXRANDOMDELAYTIME*100) * 10); }
          catch (InterruptedException e) { e.printStackTrace(); }

          (peers.get(node)).tell(new Message.Write(msg.lock, itemReq), this.getSelf());
        }
      }
    }
  }

  // d. the responsible nodes update the item

  //Last message to write the updated item
  // i. Check if the lock is the one requested
  // ii. if the lock is the one requested remove it
  // iii. Update the item
  private void onWrite(Message.Write msg){
    Item item = new Item(msg.item);
    Lock checkLock = this.locks.get(item.getKey());
    // i. Check if the lock is the one requested
    if(checkLock != null && checkLock.equals(msg.lock)) {
      // ii. if the lock is the one requested remove it
      this.locks.remove(item.getKey());
    }
    // iii. Update the item
    this.items.put(item.getKey(), item);
    System.out.println("["+this.getSelf().path().name()+"] [onWriteInformation] Owner: " + key + " ITEM: " + item);
  }

  /*----------END UPDATE----------*/

  // Print the list of nodes
  private void onPrintNodeList(Message.PrintNodeList msg){
    System.out.println("["+this.getSelf().path().name()+"] [onPrintNodeList] Node: " + key + " " + this.peers);
  }

  // Print the list of Item
  private void onPrintItemList(Message.PrintItemList msg){
    System.out.println("["+this.getSelf().path().name()+"] [onPrintItemList] Node: " + key +  "  " + this.items);
  }

  /*======================*/
}
