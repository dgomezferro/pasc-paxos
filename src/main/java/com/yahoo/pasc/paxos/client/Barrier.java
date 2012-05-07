/**
 * Copyright (c) 2011 Yahoo! Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

package com.yahoo.pasc.paxos.client;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class Barrier implements Watcher {
    private int size;
    private String name;
    private ZooKeeper zk;
    private Object mutex;
    private String root;

    /**
     * Barrier constructor
     * 
     * @param address
     * @param root
     * @param size
     * @throws IOException
     * @throws KeeperException
     */
    Barrier(ZooKeeper zk, String root, String name, int size) throws KeeperException, IOException {
        this.zk = zk;
        this.mutex = new Object();
        this.root = root;
        this.size = size;
        this.name = name;
        
        try {
            zk.create(root, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException e) {
            if (e.code().equals(Code.NODEEXISTS)) {
                //ignore
            } else {
                System.out.println("Keeper exception when instantiating barrier: " + e.toString());
            }
        } catch (InterruptedException e) {
            System.out.println("Interrupted exception");
        }
    }

    /**
     * Join barrier
     * 
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */

    boolean enter() throws KeeperException, InterruptedException {
        zk.create(root + "/" + name, null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        if (zk.getChildren(root, false).size() == size) {
            try {
                zk.create(root + "/start", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } catch (KeeperException e) {
                if (e.code().equals(Code.NODEEXISTS)) {
                    return true;
                } else throw e;
            }
            return true;
        }
        while (true) {
            synchronized (mutex) {
                Stat stat = zk.exists(root + "/start", true);
                if (stat != null) {
                    return true;
                }
                mutex.wait();
            }
        }
    }

    /**
     * Wait until all reach barrier
     * 
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    boolean leave() throws KeeperException, InterruptedException {
        zk.delete(root + "/" + name, -1);
        while (true) {
            synchronized (mutex) {
                int children = zk.getChildren(root, true).size();
                if (children == 1) {
                    try {
                          zk.delete(root + "/start", -1);
                    } catch (KeeperException e) {
                        if (e.code().equals(Code.NONODE)) {
                            return true;
                        } else throw e;
                    }
                } else if (children == 0) {
                    return true;
                } else {
                    mutex.wait();
                }
            }
        }
    }

    @Override
    public void process(org.apache.zookeeper.WatchedEvent event) {
        if (event.getType().equals(EventType.NodeChildrenChanged) ||
                event.getType().equals(EventType.NodeCreated)) {
            synchronized (mutex) {
                mutex.notify();
            }
        }
    }
    
    void close() throws InterruptedException {
        zk.close();
    }
}
