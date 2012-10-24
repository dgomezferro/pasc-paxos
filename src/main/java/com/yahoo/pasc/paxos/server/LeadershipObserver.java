package com.yahoo.pasc.paxos.server;

public interface LeadershipObserver {
    public void setLeadership(int leader);
}
