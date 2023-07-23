package it.unitn.ds1;

import akka.actor.ActorRef;

public class Request {
    ActorRef client; //client that request the operation
    Item item;
    // TO DO: add for replication controllor of R and W

    public Request(ActorRef _client, Item _item){
        client = _client;
        item = _item;
    }

    public void setItemValue(Item _item){
        item = _item;
    }

    public ActorRef getClient(){
        return client;
    }

    public Item getItem(){
        return item;
    }
}
