package it.unitn.ds1;

import akka.actor.ActorRef;

enum Type { GET, UPDATE }

public class Request {
    ActorRef client; //client that request the operation
    Item item;
    int counter; //counter used to reach R or W
    Type type;

    public Request(ActorRef _client, Item _item, Type _type){
        client = _client;
        item = _item;
        type = _type;
        counter = 0;
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

    public int getCounter(){ return counter; }

    public void setCounter(int counter) { this.counter = counter; }

    public Type getType(){
        return type;
    }
}
