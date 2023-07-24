package it.unitn.ds1;
import akka.actor.ActorRef;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.HashMap;
import java.util.HashSet;
import java.util.TreeMap;
import java.util.Set;

public class Message{
    // This class represents a message our actor will receive
    public static class Hello implements Serializable {
        public final String msg;
        public Hello(String msg) {
        this.msg = msg;
        }
    }

    // Message sent to the initiator node of the system
    public static class InitSystem implements Serializable {}

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

    // The node which is joining the network sends this message to
    // announce its presence to every node in the system
    public static class AnnouncePresence implements Serializable{
        public final int key;

        public AnnouncePresence(int _key){
            this.key = _key;
        }
    }

    // Recovery message
    public static class RecoveryMsg implements Serializable{
        public final ActorRef bootstrappingPeer;

        public RecoveryMsg(ActorRef _bootstrappingPeer){
            this.bootstrappingPeer = _bootstrappingPeer;
        }
    }

    // Ask the node to crash
    public static class CrashMsg implements Serializable {}

    // Message to trigger the print of the list of nodes
    public static class PrintNodeList implements Serializable {}

    // This class represents a message to get an item
    public static class GetRequest implements Serializable {
        public final int key;
        public GetRequest(int key) {
            this.key = key;
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
        public final int key;
        public Read(int requestId, int key) {
            this.requestId = requestId;
            this.key = key;
        }
    }

    // This class represents a message to get the version of an item
    public static class Version implements Serializable {
        public final int requestId;
        public final Item item;
        public Version(int requestId, Item item) {
            this.requestId = requestId;
            this.item = item;
        }
    }

    // This class represents a message to update an item
    public static class UpdateRequest implements Serializable {
        public final Item item;
        public UpdateRequest(Item item) {
            this.item = item;
        }
    }

    // this class represents all the information to update an item
    public static class UpdateVersion implements Serializable {
        public final int requestId;
        public final Item item;
        public UpdateVersion(int requestId, Item item) {
            this.requestId = requestId;
            this.item = item;
        }
    }

    // This class represents a message to write the new item
    public static class Write implements Serializable {
        public final int requestId;
        public final Item item;
        public Write(int requestId, Item item) {
            this.requestId = requestId;
            this.item = item;
        }
    }

    // this class represents all the information written of an item
    public static class WrittenItemInformation implements Serializable {
        public final int requestId;
        public final Item item;
        public WrittenItemInformation(int requestId, Item item) {
            this.requestId = requestId;
            this.item = item;
        }
    }
}