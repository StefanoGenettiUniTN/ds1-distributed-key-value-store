package it.unitn.ds1;
import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

public class Client extends AbstractActor {
  private ActorRef coordinator; //the node contacted by a client for a given user request is called coordinator

  public Client(ActorRef _coordinator) {
    this.coordinator = _coordinator;
  }

  @Override
  public void preStart() {
    System.out.println("client preStart");
  }

  static public Props props(ActorRef _coordinator) {
    return Props.create(Client.class, () -> new Client(_coordinator));
  }

  // Mapping between the received message types and actor methods
  @Override
  public Receive createReceive() {
    return receiveBuilder().build();
  }

  /*===MESSAGE HANDLERS===*/

  // TODO onReadResponse

  // TODO onWriteResponse

  /*======================*/

}

