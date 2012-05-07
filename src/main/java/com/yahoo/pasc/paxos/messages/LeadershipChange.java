package com.yahoo.pasc.paxos.messages;

import java.io.Serializable;

import com.yahoo.pasc.CloneableDeep;
import com.yahoo.pasc.EqualsDeep;
import com.yahoo.pasc.Message;

public class LeadershipChange extends PaxosMessage implements Serializable, CloneableDeep<LeadershipChange>,
        EqualsDeep<LeadershipChange> {
    private static final long serialVersionUID = 5876735731885428525L;

    private boolean isLeader;
    
    public LeadershipChange(boolean isLeader) {
        this.isLeader = isLeader;
    }

    public boolean isLeader() {
        return isLeader;
    }

    public void setLeader(boolean isLeader) {
        this.isLeader = isLeader;
    }

    @Override
    public boolean equalsDeep(LeadershipChange lc) {
        return lc.isLeader == this.isLeader;
    }
    @Override
    public LeadershipChange cloneDeep() {
        return new LeadershipChange(this.isLeader);
    }

    @Override
    protected boolean verify() {
        return true;
    }

    @Override
    public void storeReplica(Message m) {

    }

}
