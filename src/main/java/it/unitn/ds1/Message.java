package it.unitn.ds1;
import akka.actor.ActorRef;
import akka.actor.Cancellable;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.HashMap;
import java.util.HashSet;
import java.util.TreeMap;
import java.util.Set;

public class Message{

    // Message sent to the initiator node of the system
    public static class InitSystem implements Serializable {
        public final int key;
        
        public InitSystem(int _key){
            this.key = _key;
        }
    }

    // Join message
    public static class JoinMsg implements Serializable {
        public final int key;
        public final ActorRef bootstrappingPeer;

        public JoinMsg(int _key, ActorRef _bootstrappingPeer){
            this.key = _key;
            this.bootstrappingPeer = _bootstrappingPeer;
        }
    }

    // Bootstrap node receives this message from the node which is joining the system
    // or recovering from crash
    public static class ReqActiveNodeList implements Serializable{}

    // Bootstrap node sends current list of active nodes to the node which
    // is joining the network
    public static class ResActiveNodeList implements Serializable{
        public final Map<Integer, ActorRef> activeNodes;
        
        public ResActiveNodeList(Map<Integer, ActorRef> _activeNodes){
            this.activeNodes = Collections.unmodifiableMap(new TreeMap<>(_activeNodes));
        }
    }

    // The joining node with key=_key should request data items it is responsible for
    public static class ReqDataItemsResponsibleFor implements Serializable{
        public final int key;
        public ReqDataItemsResponsibleFor(int _key){
            this.key = _key;
        }
    }

    // The successor of the joining node sends the data items the new node is responsible for
    public static class ResDataItemsResponsibleFor implements Serializable{
        public final Set<Item> resSet;
        public ResDataItemsResponsibleFor(Set<Item> _resSet){
            this.resSet = Collections.unmodifiableSet(new HashSet<>(_resSet));
        }
    }

    // The joining node should perform read operations to ensure that its items are up to date
    public static class JoinReadOperationReq implements Serializable{
        public final Set<Item> requestItemSet;
        public JoinReadOperationReq(Set<Item> _requestItemSet){
            this.requestItemSet = Collections.unmodifiableSet(new HashSet<>(_requestItemSet));
        }
    }

    // This message is sent by nodes to the node which is joining the system. The content of the
    // message is the set of updated items that the new joining node is responsible for.
    public static class JoinReadOperationRes implements Serializable{
        public final Set<Item> responseItemSet;
        public JoinReadOperationRes(Set<Item> _responseItemSet){
            this.responseItemSet = Collections.unmodifiableSet(new HashSet<>(_responseItemSet));
        }
    }

    // The node which is joining the network sends this message to
    // announce its presence to every node in the system
    public static class AnnouncePresence implements Serializable{
        public final int key;
        public final Set<Integer> keyItemSet;

        public AnnouncePresence(int _key, Set<Integer> _keyItemSet){
            this.key = _key;
            this.keyItemSet = Collections.unmodifiableSet(new HashSet<>(_keyItemSet));
        }
    }

    // The main requests a node to leave
    public static class LeaveMsg implements Serializable{}
    
    // the node which is leaving the ring has to be sure that all its peers which
    // are going to become reponsible of a subset of its data items after the departure,
    // are not in crash state
    public static class PreLeaveStatusCheck implements Serializable{}

    // The node which is leaving announce its departure with this message
    public static class AnnounceDeparture implements Serializable{
        public final int key;
        public final Set<Item> itemSet;   // the node which is leaving passes these data items to the nodes that become responsible for them after its departure

        public AnnounceDeparture(int _key, Set<Item> _itemSet){
            this.key = _key;
            this.itemSet = Collections.unmodifiableSet(new HashSet<>(_itemSet));
        }
    }

    // this message is sent by a node which is informed by a leaving peer about new items
    // which are going to be under its responsability after the departure
    public static class DepartureAck implements Serializable{}

    // Recovery message
    public static class RecoveryMsg implements Serializable{
        public final ActorRef bootstrappingPeer;

        public RecoveryMsg(ActorRef _bootstrappingPeer){
            this.bootstrappingPeer = _bootstrappingPeer;
        }
    }

    // the node which is recovering requests the items that are now under its responsability from the clockwise neighbor.
    // In the message we include keyItemSet which is the set of items that are already in the this.items data structure
    // of the node which is recovering. In this way we avoid to send this items again from the clockwise neighbor.
    public static class ReqDataItemsResponsibleFor_recovery implements Serializable{
        public final int key;
        public final Set<Integer> keyItemSet;

        public ReqDataItemsResponsibleFor_recovery(int _key, Set<Integer> _keyItemSet){
            this.key = _key;
            this.keyItemSet = Collections.unmodifiableSet(new HashSet<>(_keyItemSet));
        }
    }

    // Ask the node to crash
    public static class CrashMsg implements Serializable {}

    // Message to trigger the print of the list of nodes
    public static class PrintNodeList implements Serializable {}

    // Message to trigger the print of the list of items
    public static class PrintItemList implements Serializable {}

    // This class represents a timeoutExpiration
    public static class Timeout implements Serializable {
        public final int requestId;
        public final Lock lock;
        public final int itemId;
        public Timeout(Lock lock, int requestId, int itemId) {
            this.requestId = requestId;
            this.lock = lock;
            this.itemId = itemId;
        }
    }

    // This class represents a message to release clocks
    public static class ReleaseLock implements Serializable {
        public final Lock lock;
        public final int itemId;
        public ReleaseLock(Lock lock, int itemId) {
            this.lock = lock;
            this.itemId = itemId;
        }
    }

    // this timeout message is sent when there is no response for ReqActiveNodeList message request
    public static class Timeout_ReqActiveNodeList implements Serializable{}

    // this timeout message is sent when there is no response for ReqActiveNodeList message request
    public static class Timeout_ReqActiveNodeList_recover implements Serializable{}

    // this timeout message is sent when there is no response for ReqDataItemsResponsibleFor message request
    public static class Timeout_ReqDataItemsResponsibleFor implements Serializable{}

    // this timeout message is sent when there is no response for ReqDataItemsResponsibleFor_recovery message request
    public static class Timeout_ReqDataItemsResponsibleFor_recovery implements Serializable{
        public final Set<Item> backupItemSet;

        public Timeout_ReqDataItemsResponsibleFor_recovery(Set<Item> _backupItemSet){
            this.backupItemSet = Collections.unmodifiableSet(new HashSet<>(_backupItemSet));
        }
    }

    // this timeout message is sent when the joining node does not receive a ReadOperationReq message from one
    // or more of the nodes that are expected
    public static class Timeout_JoinReadOperationReq implements Serializable{}
    
    // this timeout message is sent when the leaving node does not receive an acknowledgement from one or more
    // of the nodes that should become responsible of its data items after the departure
    public static class Timeout_AnnounceDeparture implements Serializable{}

    // This class represents a message to get an item
    public static class GetRequest implements Serializable {
        public final Item item;
        public GetRequest(Item item) {
            this.item = item;
        }
    }

    // this class represents all the information of an item
    public static class ReadItemInformation implements Serializable {
        public final int requestId;
        public final Item item;
        public ReadItemInformation(int requestId, Item item) {
            this.requestId = requestId;
            this.item = item;
        }
    }

    // This class represents a message to read an item
    public static class Read implements Serializable {
        public final int requestId;
        public final Item item;
        public Read(int requestId, Item item) {
            this.requestId = requestId;
            this.item = item;
        }
    }

    // This class represents a message to update an item
    public static class UpdateRequest implements Serializable {
        public final String clientName;
        public final Item item;
        public UpdateRequest(String clientName, Item item) {
            this.clientName = clientName;
            this.item = item;
        }
    }

    // This class represents a message to get the version of an item
    public static class Version implements Serializable {
        public final Lock lock;
        public final int requestId;
        public final Item item;
        public Version(Lock lock, int requestId, Item item) {
            this.lock = lock;
            this.requestId = requestId;
            this.item = item;
        }
    }

    // this class represents all the information to update an item
    public static class UpdateVersion implements Serializable {
        public final int requestId;
        public final Lock lock;
        public final Item item;
        public UpdateVersion(Lock lock, int requestId, Item item) {
            this.lock = lock;
            this.requestId = requestId;
            this.item = item;
        }
    }

    // This class represents a message to write the new item
    public static class Write implements Serializable {
        public final Lock lock;
        public final Item item;
        public Write(Lock lock, Item item) {
            this.lock = lock;
            this.item = item;
        }
    }
}