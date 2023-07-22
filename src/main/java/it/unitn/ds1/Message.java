package it.unitn.ds1;
import akka.actor.ActorRef;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.HashMap;
import java.util.TreeMap;

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

    // Bootstrap node receives this message
    // from the node which is joining the
    // system
    public static class ReqActiveNodeList implements Serializable{
        public final int key;

        public ReqActiveNodeList(int _key){
            this.key = _key;
        }
    }

    // Bootstrap node sends current list of active nodes to the node which
    // is joining the network
    public static class ResActiveNodeList implements Serializable{
        public final Map<Integer, ActorRef> activeNodes;
        
        public ResActiveNodeList(Map<Integer, ActorRef> _activeNodes){
            this.activeNodes = Collections.unmodifiableMap(new TreeMap<>(_activeNodes));
        }
    }

    // Message to trigger the print of the list of nodes
    public static class PrintNodeList implements Serializable {}
}