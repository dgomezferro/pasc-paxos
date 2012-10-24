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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LeaderElection implements Watcher {

    private static final Logger LOG = LoggerFactory.getLogger(LeaderElection.class);

    private ZooKeeper zk;
    private int id;
    private static final String ELECTION_PATH = "/pasc_election";
    private LeadershipObserver observer;
    
    public LeaderElection(ZooKeeper zk, int id, LeadershipObserver observer) throws IOException {
        this.zk = zk;
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
        
        LOG.debug("Tentative leaders: " + ids);

        if (ids.isEmpty())
            return;
        
        observer.setLeadership(ids.get(0));
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
