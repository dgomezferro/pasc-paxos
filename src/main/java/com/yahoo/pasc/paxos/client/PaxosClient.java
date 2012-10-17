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

import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.jboss.netty.handler.execution.ExecutionHandler;
import org.jboss.netty.handler.execution.OrderedMemoryAwareThreadPoolExecutor;

import com.yahoo.pasc.PascRuntime;
import com.yahoo.pasc.paxos.client.handlers.AsyncMessageHandler;
import com.yahoo.pasc.paxos.client.handlers.ByeHandler;
import com.yahoo.pasc.paxos.client.handlers.HelloHandler;
import com.yahoo.pasc.paxos.client.handlers.ReplyHandler;
import com.yahoo.pasc.paxos.client.handlers.ServerHelloHandler;
import com.yahoo.pasc.paxos.client.handlers.SubmitHandler;
import com.yahoo.pasc.paxos.client.handlers.TimeoutHandler;
import com.yahoo.pasc.paxos.client.messages.Submit;
import com.yahoo.pasc.paxos.client.messages.Timeout;
import com.yahoo.pasc.paxos.messages.AsyncMessage;
import com.yahoo.pasc.paxos.messages.Bye;
import com.yahoo.pasc.paxos.messages.Hello;
import com.yahoo.pasc.paxos.messages.Reply;
import com.yahoo.pasc.paxos.messages.ServerHello;

public class PaxosClient {
    public static void main(String[] args) throws Exception {
        
        CommandLineParser parser = new PosixParser();
        Options options;

        {
            Option id           = new Option("i", true, "client id");
            Option clients      = new Option("c", true, "number of clients");
            Option host         = new Option("l", true, "leader (hostname:port)");
            Option servers      = new Option("s", true, "number of servers");
            Option quorum       = new Option("q", true, "necesarry quorum at the client");
            Option port         = new Option("p", true, "port used by client");
            Option buffer       = new Option("b", true, "number of concurrent clients");
            Option timeout      = new Option("t", true, "timeout in milliseconds");
            Option udp          = new Option("u", false, "use UDP");
            Option zookeeper    = new Option("z", true, "zookeeper connection string");
            Option warmup       = new Option("w", true, "warmup messagges");
            Option measuring    = new Option("m", true, "measuring time");
            Option request      = new Option("r", true, "request size");
            Option frequency    = new Option("f", true, "frequency of throughput info");
            Option anm          = new Option("a", false, "use protection");
            Option inlineThresh = new Option("n", true, "threshold for sending requests iNline with accepts ");
            Option asynSize     = new Option("y", true, "size of async messages queue");
            
            options = new Options();
            options.addOption(id).addOption(host).addOption(servers).addOption(quorum).addOption(port).addOption(warmup)
                    .addOption(buffer).addOption(timeout).addOption(udp).addOption(zookeeper).addOption(clients)
                    .addOption(measuring).addOption(request).addOption(frequency).addOption(anm).addOption(inlineThresh)
                    .addOption(asynSize);
        }
        
        CommandLine line = null;
        try {
            line = parser.parse(options, args);
            
            String host         = line.hasOption('l') ? line.getOptionValue('l') : "localhost:20548,localhost:20748,localhost:20778";
            String zkConnection = line.hasOption('z') ? line.getOptionValue('z') : "localhost:2181";
            int clientIds       = line.hasOption('i') ? Integer.parseInt(line.getOptionValue('i')) : 0;
            int servers         = line.hasOption('s') ? Integer.parseInt(line.getOptionValue('s')) : 3;
            int quorum          = line.hasOption('q') ? Integer.parseInt(line.getOptionValue('q')) : 1;
            int buffer          = line.hasOption('b') ? Integer.parseInt(line.getOptionValue('b')) : 1;
            int timeout         = line.hasOption('t') ? Integer.parseInt(line.getOptionValue('t')) : 5000;
            int clients         = line.hasOption('c') ? Integer.parseInt(line.getOptionValue('c')) : 1;
            int requestSize     = line.hasOption('r') ? Integer.parseInt(line.getOptionValue('r')) : 0;
            int inlineThreshold = line.hasOption('n') ? Integer.parseInt(line.getOptionValue('n')) : 1000;
            int asynSize        = line.hasOption('y') ? Integer.parseInt(line.getOptionValue('y')) : 100;
            boolean protection  = line.hasOption('a');

            int threads = Runtime.getRuntime().availableProcessors() * 2;
            final ExecutionHandler executor = new ExecutionHandler(new OrderedMemoryAwareThreadPoolExecutor(threads, 1024 * 1024,
                1024 * 1024 * 1024, 30, TimeUnit.SECONDS, Executors.defaultThreadFactory()));

            String [] serverHosts = host.split(",");
            
            ServerHelloHandler shello = new ServerHelloHandler();
            ReplyHandler reply = new ReplyHandler();
            SubmitHandler submit = new SubmitHandler();
            TimeoutHandler tout = new TimeoutHandler();
            AsyncMessageHandler asyncm = new AsyncMessageHandler();
            ByeHandler bye = new ByeHandler();
            HelloHandler hello = new HelloHandler();
            
            Random rnd = new Random();
            
            for (int i = 0; i < buffer; ++ i) {
                int clientId = clientIds++;
                ClientState clientState = new ClientState(clientId, servers, quorum, inlineThreshold, asynSize);
                final PascRuntime<ClientState> runtime = new PascRuntime<ClientState>(protection);
                runtime.setState(clientState);
                runtime.addHandler(ServerHello.class, shello);
                runtime.addHandler(Reply.class, reply);
                runtime.addHandler(Submit.class, submit);
                runtime.addHandler(Timeout.class, tout);
                runtime.addHandler(AsyncMessage.class, asyncm);
                runtime.addHandler(Bye.class, bye);
                runtime.addHandler(Hello.class, hello);
                
                final PaxosClientHandler handler = new PaxosClientHandler(runtime, new SimpleClient(requestSize), 
                        serverHosts, clientId, clients, timeout, zkConnection, executor);
                
                if (line.hasOption('w')) handler.setWarmup(Integer.parseInt(line.getOptionValue('w')));
                if (line.hasOption('m')) handler.setMeasuringTime(Integer.parseInt(line.getOptionValue('m')));
                if (line.hasOption('f')) handler.setPeriod(Integer.parseInt(line.getOptionValue('f')));

                handler.start();

                Thread.sleep(rnd.nextInt(200));
            }
        } catch (Exception e) {
            System.err.println("Unexpected exception " + e);
            e.printStackTrace();
            
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("Paxos", options);
            
            System.exit(-1);
        }
    }
}
