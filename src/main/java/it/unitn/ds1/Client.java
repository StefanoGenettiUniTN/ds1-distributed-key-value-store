package it.unitn.ds1;
import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

public class Client extends AbstractActor {

  public Client() {}

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
    System.out.println("["+this.getSelf().path().name()+"] [onGet] Client");
    (msg.coordinator).tell(new Message.GetRequest(msg.item), this.getSelf());
  }

  private void onGetResult(ClientMessage.GetResult msg){
    if(msg.result == Result.SUCCESS) {
      System.out.println("["+this.getSelf().path().name()+"] [onGetResult] Client: " + msg.item);
    } else {
      System.out.println("[" + this.getSelf().path().name() + "] [onGetResult] Client: ERROR!");
    }
  }

  // TODO onWriteResponse
  // Ask the coordinator to update an item
  private void onUpdate(ClientMessage.Update msg){
    System.out.println("["+this.getSelf().path().name()+"] [onUpdate] Client");
    (msg.coordinator).tell(new Message.UpdateRequest(msg.item), this.getSelf());
  }

  private void onUpdateResult(ClientMessage.UpdateResult msg){
    if(msg.result == Result.SUCCESS) {
      System.out.println("[" + this.getSelf().path().name() + "] [onUpdateResult] Client: " + msg.item);
    } else {
      System.out.println("[" + this.getSelf().path().name() + "] [onUpdateResult] Client: ERROR!");
    }
  }

  /*======================*/

}

