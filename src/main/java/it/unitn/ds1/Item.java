package it.unitn.ds1;

public class Item {
    int key;
    String value;
    int version;

    public Item(int _key, String _value){
        key = _key;
        value = _value;
        version = 0;
    }

    public Item(int _key, String _value, int _version){
        key = _key;
        value = _value;
        version = _version;
    }

    public Item(Item that){
        this(that.getKey(), that.getValue(), that.getVersion());
    }

    public void setKey(int _key){
        key = _key;
    }

    public void setValue(String _value){
        value = _value;
    }

    public void setVersion(int _version){ version = _version; }

    public int getKey(){
        return key;
    }

    public String getValue(){
        return value;
    }

    public int getVersion(){
        return version;
    }

    @Override
    public String toString() {
        return "Item{" +
                "key=" + key +
                ", value='" + value + '\'' +
                ", version=" + version +
                '}';
    }
}
