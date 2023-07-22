package it.unitn.ds1;

public class Item {
    int key;
    String value;
    int version;

    public Item(int _key, String _value, int _version){
        key = _key;
        value = _value;
        version = _version;
    }

    public void setKey(int _key){
        key = _key;
    }

    public void setValue(String _value){
        value = _value;
    }

    public void setVersion(int _version){
        version = _version;
    }

    public int getKey(){
        return key;
    }

    public String getValue(){
        return value;
    }

    public int getVersion(){
        return version;
    }
}
