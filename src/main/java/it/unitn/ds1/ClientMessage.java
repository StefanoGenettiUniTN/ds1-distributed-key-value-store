package it.unitn.ds1;
import akka.actor.ActorRef;
import java.io.Serializable;

enum Result { SUCCESS, ERROR }

public class ClientMessage{
    // This class represents a message to request a get operation
    public static class Get implements Serializable {
        public final Item item;
        public final ActorRef coordinator; //Node responsible for the get operation
        public Get(Item item, ActorRef coordinator) {
            this.item = item;
            this.coordinator = coordinator;
        }
    }

    // This class represents a message to return the response of a get operation
    public static class GetResult implements Serializable {
        public final Result result;  //Operation succeeds or error
        public final Item item;
        public GetResult(Result result, Item item) {
            this.result = result;
            this.item = item;
        }
    }

    // This class represents the request message to update an item
    public static class Update implements Serializable {
        public final Item item;
        public final ActorRef coordinator; //Node responsible for the update operation
        public Update(Item item, ActorRef coordinator) {
            this.item = item;
            this.coordinator = coordinator;
        }
    }

    // This class represents a message to get return the result of an update of an item
    public static class UpdateResult implements Serializable {
        public final Result result; //Operation succeeds or error
        public final Item item;
        public UpdateResult(Result result, Item item) {
            this.result = result;
            this.item = item;
        }
    }
}