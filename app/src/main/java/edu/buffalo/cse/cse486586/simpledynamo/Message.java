package edu.buffalo.cse.cse486586.simpledynamo;



public class Message implements Comparable<Message> {

    private String key;
    private String value;
    private String origin;
    private MessageType messageType;
    private String originTimestamp;


    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getOrigin() {
        return origin;
    }

    public void setOrigin(String origin) {
        this.origin = origin;
    }

    public MessageType getMessageType() {
        return messageType;
    }

    public void setMessageType(MessageType messageType) {
        this.messageType = messageType;
    }

    public String getOriginTimestamp() {
        return originTimestamp;
    }

    public void setOriginTimestamp(String originTimestamp) {
        this.originTimestamp = originTimestamp;
    }

    public Message() {
    }

    public Message(String packet){

        String strReceived [] = packet.trim().split(Constants.SEPARATOR);

        key = strReceived[0];
        value = strReceived[1];
        origin = strReceived[2];
        messageType = MessageType.valueOf(strReceived[3]);
        originTimestamp = strReceived[4];

    }

    public String createPacket(){

        return key + Constants.SEPARATOR + value + Constants.SEPARATOR + origin +
                Constants.SEPARATOR + messageType.name() + Constants.SEPARATOR + originTimestamp;

    }

    @Override
    public String toString() {
        return "Message{" +
                "key='" + key + '\'' +
                ", value='" + value + '\'' +
                ", origin='" + origin + '\'' +
                ", messageType=" + messageType +
                ", originTimestamp='" + originTimestamp + '\'' +
                '}';
    }

    @Override
    public int compareTo(Message another) {



        int result = this.key.compareTo(another.key);

        if(result == 0){
            result = this.originTimestamp.compareTo(another.originTimestamp);
        }

        return result;
    }

}
