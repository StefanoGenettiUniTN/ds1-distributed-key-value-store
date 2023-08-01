package it.unitn.ds1;
import akka.actor.AbstractActor;
import akka.actor.Props;

import java.util.Random;

public class Client extends AbstractActor {

  private boolean ongoingOperation = false; //Used to understand if other operations are executing
  private final Random rnd;
  private final int MAXRANDOMDELAYTIME = 1; //Maximum delay time in seconds

  public Client() {
    this.ongoingOperation  = false;
    this.rnd = new Random();
  }

  @Override
  public void preStart() {
    System.out.println("client preStart");
  }

  static public Props props() {
    return Props.create(Client.class, () -> new Client());
  }

  // Mapping between the received message types and actor methods
  @Override
  public Receive createReceive() {
    return receiveBuilder()
            .match(ClientMessage.Get.class, this::onGet)
            .match(ClientMessage.GetResult.class, this::onGetResult)
            .match(ClientMessage.Update.class, this::onUpdate)
            .match(ClientMessage.UpdateResult.class, this::onUpdateResult)
            .build();
  }

  /*===MESSAGE HANDLERS===*/

  // Ask the coordinator to get an item
  private void onGet(ClientMessage.Get msg){
    //Check if no other operations are executing
    if(this.ongoingOperation == false) {
      //Execute operation
      this.ongoingOperation = true;
      System.out.println("[" + this.getSelf().path().name() + "] [onGet] Client");

      // model a random network/processing delay
      try { Thread.sleep(rnd.nextInt(this.MAXRANDOMDELAYTIME*100) * 10); }
      catch (InterruptedException e) { e.printStackTrace(); }

      //Ask the coordinator to perform the request
      (msg.coordinator).tell(new Message.GetRequest(this.getSelf().path().name(), msg.item), this.getSelf());
    } else{
      //If another operation is executing, stop and return the message
      System.out.println("ERR: ongoing operations, item " + msg.item);
    }
  }

  private void onGetResult(ClientMessage.GetResult msg){
    //Release "lock" on client
    this.ongoingOperation = false;

    //Check if the operations have succeeded or aborted due to timeout expiration or other error
    if(msg.result == Result.SUCCESS) {
      System.out.println("["+this.getSelf().path().name()+"] [onGetResult] Client: " + msg.item);
    } else {
      System.out.println("[" + this.getSelf().path().name() + "] [onGetResult] Client: ERROR!");
    }
  }

  // Ask the coordinator to update an item
  private void onUpdate(ClientMessage.Update msg){
    //Check if no other operations are executing
    if(this.ongoingOperation == false) {
      //Execute operation
      this.ongoingOperation = true;
      System.out.println("[" + this.getSelf().path().name() + "] [onUpdate] Client");

      // model a random network/processing delay
      try { Thread.sleep(rnd.nextInt(this.MAXRANDOMDELAYTIME*100) * 10); }
      catch (InterruptedException e) { e.printStackTrace(); }

      //Ask to the coordinator to perform the update
      (msg.coordinator).tell(new Message.UpdateRequest(this.getSelf().path().name(), msg.item), this.getSelf());
    } else{
      //If another operation is executing, stop and return the message
      System.out.println("ERR: ongoing operations, item " + msg.item);
    }
  }

  private void onUpdateResult(ClientMessage.UpdateResult msg){
    //Release "lock" on client
    this.ongoingOperation = false;

    //Check if the operations have succeeded or aborted due to timeout expiration or other error
    if(msg.result == Result.SUCCESS) {
      System.out.println("[" + this.getSelf().path().name() + "] [onUpdateResult] Client: " + msg.item);
    } else {
      System.out.println("[" + this.getSelf().path().name() + "] [onUpdateResult] Client: ERROR!");
    }
  }

  /*======================*/

}

