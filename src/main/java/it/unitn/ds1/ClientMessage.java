package it.unitn.ds1;
import akka.actor.ActorRef;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.HashMap;
import java.util.TreeMap;

public class ClientMessage{
    // This class represents a message to get an item value
    public static class Get implements Serializable {
        public final int key;
        public Get(int key) {
            this.key = key;
        }
    }

    // This class represents a message to get an item value
    public static class GetResult implements Serializable {
        public final Item item;
        public GetResult(Item item) {
            this.item = item;
        }
    }

    // This class represents a message to update an item
    public static class Update implements Serializable {
        public final Item item;
        public Update(Item item) {
            this.item = item;
        }
    }

    // This class represents a message to update an item
    public static class UpdateResult implements Serializable {
        public final Item item;
        public UpdateResult(Item item) {
            this.item = item;
        }
    }
}