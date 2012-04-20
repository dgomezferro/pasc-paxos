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

package com.yahoo.paxos.client;

import java.net.InetSocketAddress;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ConnectionlessBootstrap;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.DatagramChannel;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioDatagramChannelFactory;
import org.jboss.netty.handler.execution.ExecutionHandler;
import org.jboss.netty.handler.execution.OrderedMemoryAwareThreadPoolExecutor;

import com.yahoo.pasc.PascRuntime;
import com.yahoo.paxos.client.handlers.HelloHandler;
import com.yahoo.paxos.client.handlers.ReplyHandler;
import com.yahoo.paxos.client.handlers.SubmitHandler;
import com.yahoo.paxos.client.handlers.TimeoutHandler;
import com.yahoo.paxos.client.messages.Submit;
import com.yahoo.paxos.client.messages.Timeout;
import com.yahoo.paxos.messages.Hello;
import com.yahoo.paxos.messages.Reply;
import com.yahoo.paxos.messages.serialization.ManualDecoder;
import com.yahoo.paxos.messages.serialization.ManualEncoder;

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
            
            options = new Options();
            options.addOption(id).addOption(host).addOption(servers).addOption(quorum).addOption(port).addOption(warmup)
                    .addOption(buffer).addOption(timeout).addOption(udp).addOption(zookeeper).addOption(clients)
                    .addOption(measuring).addOption(request).addOption(frequency).addOption(anm).addOption(inlineThresh);
        }
        
        CommandLine line = null;
        try {
            line = parser.parse(options, args);
            
            String host         = line.hasOption('l') ? line.getOptionValue('l') : "localhost:20548,localhost:20748,localhost:20778";
            int clientIds       = line.hasOption('i') ? Integer.parseInt(line.getOptionValue('i')) : 0;
            int servers         = line.hasOption('s') ? Integer.parseInt(line.getOptionValue('s')) : 3;
            int quorum          = line.hasOption('q') ? Integer.parseInt(line.getOptionValue('q')) : 1;
            int buffer          = line.hasOption('b') ? Integer.parseInt(line.getOptionValue('b')) : 1;
            int clientPort      = line.hasOption('p') ? Integer.parseInt(line.getOptionValue('p')) : 9000;
            int timeout         = line.hasOption('t') ? Integer.parseInt(line.getOptionValue('t')) : 5000;
            int clients         = line.hasOption('c') ? Integer.parseInt(line.getOptionValue('c')) : 1;
            int requestSize     = line.hasOption('r') ? Integer.parseInt(line.getOptionValue('r')) : 0;
            int inlineThreshold = line.hasOption('n') ? Integer.parseInt(line.getOptionValue('n')) : 10;
            boolean protection  = line.hasOption('a');
            boolean udp         = line.hasOption('u');
            String zkConnection = line.getOptionValue('z');
            
    
            int threads = Runtime.getRuntime().availableProcessors() * 2;
            final ExecutionHandler executor = new ExecutionHandler(new OrderedMemoryAwareThreadPoolExecutor(threads, 1024 * 1024,
                1024 * 1024 * 1024, 30, TimeUnit.SECONDS, Executors.defaultThreadFactory()));
            final ManualEncoder encoder = new ManualEncoder();
            
            String [] serverHosts = host.split(",");
            
            HelloHandler hello = new HelloHandler();
            ReplyHandler reply = new ReplyHandler();
            SubmitHandler submit = new SubmitHandler();
            TimeoutHandler tout = new TimeoutHandler();
            
            Random rnd = new Random();
            
            for (int i = 0; i < buffer; ++ i) {
                int clientId = clientIds++;
                ClientState clientState = new ClientState(clientId, servers, quorum, inlineThreshold);
                final PascRuntime<ClientState> runtime = new PascRuntime<ClientState>(protection);
                runtime.setState(clientState);
                runtime.addHandler(Hello.class, hello);
                runtime.addHandler(Reply.class, reply);
                runtime.addHandler(Submit.class, submit);
                runtime.addHandler(Timeout.class, tout);
                
                final PaxosClientHandler handler = new PaxosClientHandler(runtime, new SimpleClient(requestSize), 
                        clientId, clients, timeout, zkConnection);
                
                if (line.hasOption('w')) handler.setWarmup(Integer.parseInt(line.getOptionValue('w')));
                if (line.hasOption('m')) handler.setMeasuringTime(Integer.parseInt(line.getOptionValue('m')));
//                if (line.hasOption('r')) handler.setRequestSize(Integer.parseInt(line.getOptionValue('r')));
                if (line.hasOption('f')) handler.setPeriod(Integer.parseInt(line.getOptionValue('f')));
                
                ChannelPipelineFactory channelPipelineFactory = new ChannelPipelineFactory() {
                    public ChannelPipeline getPipeline() throws Exception {
    //                  return Channels.pipeline(new KryoEncoder(kryo), new KryoDecoder(kryo), executor, handler);
                      return Channels.pipeline(encoder, new ManualDecoder(), executor, handler);
//                           return Channels.pipeline(new ObjectEncoder(), new ObjectDecoder(), executor, handler);
                    }
                };
                
                if (udp) {
                    startUdpClient(channelPipelineFactory, clientPort, serverHosts);
                } else {
                    startTcpClient(channelPipelineFactory, clientPort, serverHosts);
                }
                
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

    private static void startUdpClient(ChannelPipelineFactory channelPipelineFactory, int clientPort, String[] hosts) {
        
        // Configure the client.
        ConnectionlessBootstrap bootstrap = new ConnectionlessBootstrap(new NioDatagramChannelFactory(
                Executors.newCachedThreadPool()));
        
        // Set up the pipeline factory.
        bootstrap.setPipelineFactory(channelPipelineFactory);
   
        // Start the connection attempt.
        bootstrap.bind(new InetSocketAddress(clientPort));
        
        for (String host : hosts) {
            final String s[] = host.split(":");
            final String hostname = s[0];
            final int port = Integer.parseInt(s[1]);
            System.out.println("Connecting to " + hostname + ":" + port);
            DatagramChannel clientChannel = (DatagramChannel) bootstrap.bind(new InetSocketAddress(0));
            clientChannel.connect(new InetSocketAddress(hostname, port)).awaitUninterruptibly();
        }
    }

    private static void startTcpClient(ChannelPipelineFactory channelPipelineFactory, int clientPort, String [] hosts) {
        
//        startServer(channelPipelineFactory, clientPort);

        // Configure the client.
        ClientBootstrap bootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(
                Executors.newCachedThreadPool(), Executors.newCachedThreadPool()));

        // Set up the pipeline factory.
        bootstrap.setPipelineFactory(channelPipelineFactory);
        bootstrap.setOption("tcpNoDelay", true);
        bootstrap.setOption("keepAlive", true);

        for (String host : hosts) {
            final String s[] = host.split(":");
            final String hostname = s[0];
            final int port = Integer.parseInt(s[1]);
            System.out.println("Connecting to " + hostname + ":" + port);
            // Start the connection attempt.
            bootstrap.connect(new InetSocketAddress(hostname, port));
        }
    }
}
