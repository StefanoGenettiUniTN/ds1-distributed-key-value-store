package it.unitn.ds1;

import akka.actor.ActorRef;

enum Type { GET, UPDATE }

public class Request {
    ActorRef client;        // client that request the operation
    Item item;              // Item relative to the request
    String clientName;      // name of the client that made the request
    int operationCounter;   // number of replies relative to the request
    Type type;              // type of operation

    public Request(ActorRef _client, Item _item, Type _type, String _clientName){
        client = _client;
        item = _item;
        type = _type;
        clientName = _clientName;
        operationCounter = 0;
    }

    public void setItemValue(Item _item){
        item = _item;
    }

    public int getOperationCounter() {
        return operationCounter;
    }

    public void setOperationCounter(int operationCounter) {
        this.operationCounter = operationCounter;
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
