package it.unitn.ds1;
import akka.actor.ActorRef;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.HashMap;
import java.util.TreeMap;

enum Result { SUCCESS, ERROR }

public class ClientMessage{
    // This class represents a message to get an item value
    public static class Get implements Serializable {
        public final int key;
        public final ActorRef coordinator;
        public Get(int key, ActorRef coordinator) {
            this.key = key;
            this.coordinator = coordinator;
        }
    }

    // This class represents a message to get an item value
    public static class GetResult implements Serializable {
        public final Result result;
        public final Item item;
        public GetResult(Result result, Item item) {
            this.result = result;
            this.item = item;
        }
    }

    // This class represents a message to update an item
    public static class Update implements Serializable {
        public final Item item;
        public final ActorRef coordinator;
        public Update(Item item, ActorRef coordinator) {
            this.item = item;
            this.coordinator = coordinator;
        }
    }

    // This class represents a message to update an item
    public static class UpdateResult implements Serializable {
        public final Result result;
        public final Item item;
        public UpdateResult(Result result, Item item) {
            this.result = result;
            this.item = item;
        }
    }
}