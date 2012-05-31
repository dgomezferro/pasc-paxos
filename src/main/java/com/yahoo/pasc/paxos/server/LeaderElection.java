package com.yahoo.pasc.paxos.server;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

public class LeaderElection implements Watcher {
    
    private ZooKeeper zk;
    private int id;
    private static final String ELECTION_PATH = "/pasc_election";
    private boolean leader = false;
    private LeadershipObserver observer;
    
    public LeaderElection(String zk, int id, LeadershipObserver observer) throws IOException {
        this.zk = new ZooKeeper(zk, 2000, this);
        this.id = id;
        this.observer = observer;
    }

    public void start() {
        try {
            zk.create(ELECTION_PATH, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException.NoNodeException e) {
            // ignore
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            zk.create(ELECTION_PATH + "/" + id, null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
//            checkLeadership(zk.getChildren(ELECTION_PATH, this));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void checkLeadership(List<String> children) {
        List<Integer> ids = new ArrayList<Integer>();
        for (String id : children) {
            ids.add(Integer.parseInt(id));
        }
        Collections.sort(ids);
        
        if (ids.isEmpty())
            return;
        
        boolean chosenLeader = ids.get(0) == this.id;
        
        if (chosenLeader && !leader) {
            leader = true;
            observer.acquireLeadership();
        }
        if (!chosenLeader && leader) {
            leader = false;
            observer.releaseLeadership();
        }
    }

    @Override
    public void process(WatchedEvent event) {
        if (event.getType() != EventType.NodeChildrenChanged)
            return;
        refresh();
    }

    public void refresh() {
        try {
            checkLeadership(zk.getChildren(ELECTION_PATH, this));
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    
    
}
