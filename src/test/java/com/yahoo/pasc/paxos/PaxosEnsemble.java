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

package com.yahoo.pasc.paxos;

import java.io.File;
import java.io.IOException;

import org.apache.zookeeper.server.ZooKeeperServerMain;

import com.yahoo.pasc.paxos.client.PaxosClient;
import com.yahoo.pasc.paxos.server.PaxosServer;


public class PaxosEnsemble {
    private static void executeServer(final String name, final String conf) {
        Thread t = new Thread(name) {
            @Override
            public void run() {
                System.setProperty("serverName", name);
                try {
                    PaxosServer.main(conf.split(" "));
                } catch (Exception e) {
                    e.printStackTrace();
                    System.exit(-1);
                }
            }
        };
        t.start();
    }
    private static void executeClient(final String name, final String conf) {
        Thread t = new Thread(name) {
            @Override
            public void run() {
                System.setProperty("serverName", name);
                try {
                    PaxosClient.main(conf.split(" "));
                } catch (Exception e) {
                    e.printStackTrace();
                    System.exit(-1);
                }
            }
        };
        t.start();
    }

    public static void main(String[] args) throws InterruptedException, IOException {
        String useAnm = "";
        useAnm = " -a ";
//      String confServerCommon = "-s 127.0.0.1:20548,127.0.0.1:20748,127.0.0.1:20778 -c 127.0.0.1:9000,127.0.0.1:9001,127.0.0.1:9002 -t 1 -b 10 " + useAnm;
      String confServerCommon = "-s 127.0.0.1:20548,127.0.0.1:20748,127.0.0.1:20778 -c 127.0.0.1:9000 -t 1 -b 10 -m 1024 -k 16 " + useAnm;
        String confServer0 = "-i 0 -p 20548 " + confServerCommon;
        String confServer1 = "-i 1 -p 20748 -r " + confServerCommon;
        String confServer2 = "-i 2 -p 20778 " + confServerCommon;

        String confClient = "-i 0 -l 127.0.0.1:20548,127.0.0.1:20748,127.0.0.1:20778, -s 3 -c 1 -r 10  -w 1000000" + useAnm;
//        String confClient2 = "-i 1 -l 127.0.0.1:20548,127.0.0.1:20748,127.0.0.1:20778, -s 3 -p 9001 -c 3 " + useAnm;
//        String confClient3 = "-i 2 -l 127.0.0.1:20548,127.0.0.1:20748,127.0.0.1:20778, -s 3 -p 9002 -c 3 " + useAnm;

        if (args.length == 0) {
        startZK();

        executeServer("leader", confServer0);
        Thread.sleep(1000);
        executeServer("replica1", confServer1);
        Thread.sleep(1000);
        executeServer("replica2", confServer2);
        
        Thread.sleep(7000);

        executeClient("client", confClient);
//        executeClient("client2", confClient2);
//        executeClient("client3", confClient3);
        } else {
            if (args[0].equals("zk")) startZK();
            if (args[0].equals("0")) executeServer("rep0", confServer0);
            if (args[0].equals("1")) executeServer("rep1", confServer1);
            if (args[0].equals("2")) executeServer("rep2", confServer2);
            if (args[0].equals("c")) executeClient("client", confClient);
        }
    }
    private static void startZK() throws IOException {
        final File zkTmpDir = File.createTempFile("zookeeper", "test");
        zkTmpDir.delete();
        zkTmpDir.mkdir();

        new Thread() {
            @Override
            public void run() {
                ZooKeeperServerMain.main(new String [] {"2181",  zkTmpDir.getAbsolutePath()});
            }
        }.start();

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
        }
    }
}
