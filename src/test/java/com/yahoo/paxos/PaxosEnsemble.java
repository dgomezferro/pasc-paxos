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

package com.yahoo.paxos;

import com.yahoo.paxos.client.PaxosClient;
import com.yahoo.paxos.server.PaxosServer;


public class PaxosEnsemble {
    private static void executeServer(String name, final String conf) {
        new Thread(name) {
            @Override
            public void run() {
                try {
                    PaxosServer.main(conf.split(" "));
                } catch (Exception e) {
                    e.printStackTrace();
                    System.exit(-1);
                }
            }
        }.start();
    }
    private static void executeClient(String name, final String conf) {
        new Thread(name) {
            @Override
            public void run() {
                try {
                    PaxosClient.main(conf.split(" "));
                } catch (Exception e) {
                    e.printStackTrace();
                    System.exit(-1);
                }
            }
        }.start();
    }

    public static void main(String[] args) throws InterruptedException {
        String useAnm = "";
        useAnm = " -a ";
        String confServerCommon = "-s 127.0.0.1:20548,127.0.0.1:20748,127.0.0.1:20778 -c 127.0.0.1:9000,127.0.0.1:9001,127.0.0.1:9002 -t 1 -b 10 " + useAnm;
        String confServer0 = "-i 0 -l -p 20548 " + confServerCommon;
        String confServer1 = "-i 1 -p 20748 " + confServerCommon;
        String confServer2 = "-i 2 -p 20778 " + confServerCommon;

        String confClient = "-i 0 -l 127.0.0.1:20548,127.0.0.1:20748,127.0.0.1:20778, -s 3 -c 3 " + useAnm;
        String confClient2 = "-i 1 -l 127.0.0.1:20548,127.0.0.1:20748,127.0.0.1:20778, -s 3 -p 9001 -c 3 " + useAnm;
        String confClient3 = "-i 2 -l 127.0.0.1:20548,127.0.0.1:20748,127.0.0.1:20778, -s 3 -p 9002 -c 3 " + useAnm;

        executeServer("leader", confServer0);
//        Thread.sleep(6000);
        executeServer("replica1", confServer1);
//        Thread.sleep(2000);
        executeServer("replica2", confServer2);
        
        Thread.sleep(10000);

        executeClient("client", confClient);
        executeClient("client2", confClient2);
        executeClient("client3", confClient3);
    }
}
