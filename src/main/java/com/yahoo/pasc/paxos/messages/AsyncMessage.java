package com.yahoo.pasc.paxos.messages;

import java.util.Arrays;

import com.yahoo.pasc.CloneableDeep;
import com.yahoo.pasc.EqualsDeep;

public class AsyncMessage extends PaxosMessage implements CloneableDeep<AsyncMessage>, EqualsDeep<AsyncMessage> {
    private static final long serialVersionUID = -8514053535510009861L;

    private int clientId;
    private int serverId;
    private long timestamp;
    private byte[] message;

    public AsyncMessage(int clientId, int serverId, long timestamp, byte[] message) {
        super();
        this.clientId = clientId;
        this.serverId = serverId;
        this.timestamp = timestamp;
        this.message = message;
    }

    public int getClientId() {
        return clientId;
    }

    public void setClientId(int clientId) {
        this.clientId = clientId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public int getServerId() {
        return serverId;
    }

    public void setServerId(int serverId) {
        this.serverId = serverId;
    }

    public byte[] getMessage() {
        return message;
    }

    public void setMessage(byte[] message) {
        this.message = message;
    }

    @Override
    public AsyncMessage cloneDeep() {
        byte[] msgArray = new byte[this.message.length];
        System.arraycopy(this.message, 0, msgArray, 0, msgArray.length);
        return new AsyncMessage(this.clientId, this.serverId, this.timestamp, msgArray);
    }

    @Override
    public boolean equalsDeep(AsyncMessage other) {
        if (!Arrays.equals(other.message, this.message)) {
            return false;
        }
        return (this.clientId == other.clientId && this.serverId == other.serverId && this.timestamp == other.timestamp);
    }
}
