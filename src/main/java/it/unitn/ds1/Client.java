package it.unitn.ds1;
import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

import java.util.Random;

public class Client extends AbstractActor {

  private boolean ongoingOperation = false;
  private final Random rnd;
  private final int MAXRANDOMDELAYTIME = 3;

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

  // TODO onReadResponse
  // Ask the coordinator to get an item
  private void onGet(ClientMessage.Get msg){
    if(this.ongoingOperation == false) {
      this.ongoingOperation = true;
      System.out.println("[" + this.getSelf().path().name() + "] [onGet] Client");

      // model a random network/processing delay
      try { Thread.sleep(rnd.nextInt(this.MAXRANDOMDELAYTIME)); }
      catch (InterruptedException e) { e.printStackTrace(); }

      (msg.coordinator).tell(new Message.GetRequest(msg.item), this.getSelf());
    } else{
      System.out.println("ERR: ongoing operations, item " + msg.item);
    }
  }

  private void onGetResult(ClientMessage.GetResult msg){
    this.ongoingOperation = false;
    if(msg.result == Result.SUCCESS) {
      System.out.println("["+this.getSelf().path().name()+"] [onGetResult] Client: " + msg.item);
    } else {
      System.out.println("[" + this.getSelf().path().name() + "] [onGetResult] Client: ERROR!");
    }
  }

  // TODO onWriteResponse
  // Ask the coordinator to update an item
  private void onUpdate(ClientMessage.Update msg){
    if(this.ongoingOperation == false) {
      this.ongoingOperation = true;
      System.out.println("[" + this.getSelf().path().name() + "] [onUpdate] Client");

      // model a random network/processing delay
      try { Thread.sleep(rnd.nextInt(this.MAXRANDOMDELAYTIME)); }
      catch (InterruptedException e) { e.printStackTrace(); }

      (msg.coordinator).tell(new Message.UpdateRequest(msg.item), this.getSelf());
    } else{
      System.out.println("ERR: ongoing operations, item " + msg.item);
    }
  }

  private void onUpdateResult(ClientMessage.UpdateResult msg){
    this.ongoingOperation = false;
    if(msg.result == Result.SUCCESS) {
      System.out.println("[" + this.getSelf().path().name() + "] [onUpdateResult] Client: " + msg.item);
    } else {
      System.out.println("[" + this.getSelf().path().name() + "] [onUpdateResult] Client: ERROR!");
    }
  }

  /*======================*/

}

