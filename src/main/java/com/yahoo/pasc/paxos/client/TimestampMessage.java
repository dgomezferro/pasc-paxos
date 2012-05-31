package com.yahoo.pasc.paxos.client;

import java.util.Arrays;
import java.util.BitSet;

import com.yahoo.pasc.CloneableDeep;
import com.yahoo.pasc.EqualsDeep;

public class TimestampMessage implements CloneableDeep<TimestampMessage>, EqualsDeep<TimestampMessage> {
    long timestamp;
    byte[] message;
    BitSet acks;
    boolean delivered;

    public TimestampMessage(long timestamp, byte[] message) {
        super();
        this.timestamp = timestamp;
        this.message = message;
        this.acks = new BitSet();
        this.delivered = false;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public byte[] getMessage() {
        return message;
    }

    public void setMessage(byte[] message) {
        this.message = message;
    }

    public BitSet getAcks() {
        return acks;
    }

    public void setAcks(BitSet acks) {
        this.acks = acks;
    }

    public boolean isDelivered() {
        return delivered;
    }

    public void setDelivered(boolean delivered) {
        this.delivered = delivered;
    }

    @Override
    public TimestampMessage cloneDeep() {
        byte[] copyMsg = new byte[message.length];
        System.arraycopy(message, 0, copyMsg, 0, message.length);
        BitSet copyAcks = (BitSet) acks.clone();
        TimestampMessage tm = new TimestampMessage(timestamp, copyMsg);
        tm.setAcks(copyAcks);
        tm.setDelivered(delivered);
        return tm;
    }

    @Override
    public boolean equalsDeep(TimestampMessage other) {
        if (!Arrays.equals(message, other.message)) {
            return false;
        }
        if (!acks.equals(other.acks)) {
            return false;
        }
        return timestamp == other.timestamp && delivered == other.delivered;
    }

}
