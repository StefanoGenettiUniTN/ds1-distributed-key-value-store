package it.unitn.ds1;

import akka.actor.ActorRef;

enum Type { GET, UPDATE }

public class Request {
    ActorRef client; //client that request the operation
    Item item;
    Type type;
    // TO DO: add for replication controllor of R and W

    public Request(ActorRef _client, Item _item, Type _type){
        client = _client;
        item = _item;
        type = _type;
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

    public Type getType(){
        return type;
    }
}
