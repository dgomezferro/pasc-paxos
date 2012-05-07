package com.yahoo.pasc.paxos.server;

public interface LeadershipObserver {
    public void acquireLeadership();

    public void releaseLeadership();
}
