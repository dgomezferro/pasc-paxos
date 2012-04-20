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

package com.yahoo.paxos.server.udp;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ConnectionlessBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.socket.DatagramChannel;
import org.jboss.netty.channel.socket.nio.NioDatagramChannelFactory;

import com.yahoo.pasc.Message;
import com.yahoo.pasc.PascRuntime;
import com.yahoo.paxos.messages.Reply;
import com.yahoo.paxos.server.PipelineFactory;
import com.yahoo.paxos.server.ServerConnection;
import com.yahoo.paxos.state.PaxosState;

public class UdpServer implements ServerConnection {

    private String servers[];
    private String clients[];

    private ChannelPipelineFactory channelPipelineFactory;
    private ChannelFactory channelFactory;

    private Set<DatagramChannel> serverAddresses = new HashSet<DatagramChannel>();
    private Map<Integer, InetSocketAddress> clientAddresses = new HashMap<Integer, InetSocketAddress>(5);
    private DatagramChannel channel;
//    private DatagramChannel sendChannel;
    private int port;
//    private int threads;

    public UdpServer(PascRuntime<PaxosState> runtime, String servers[], String clients[], int port, int threads, int id) {
        this.channelFactory = new NioDatagramChannelFactory(Executors.newCachedThreadPool());
        this.channelPipelineFactory = new PipelineFactory(runtime, null, this, threads, id, true);
        this.servers = servers;
        this.clients = clients;
        this.port = port;
//        this.threads = threads;
    }

    public void run() throws IOException {
        startServer();
        setupConnections();
    }

    @Override
    public void forward(List<Message> messages) {
        if (messages == null) {
            return;
        }
        
        for (Message msg : messages) {
            if (msg == null) {
                continue;
            }
            
            if (msg instanceof Reply) {
                Reply reply = (Reply) msg;
                channel.write(reply, clientAddresses.get(reply.getClientId()));
                
            } else {
                // Send to all servers
                for (DatagramChannel channel : serverAddresses) {
                    channel.write(msg);
//                    future.awaitUninterruptibly();
//                    System.out.println("Sent " + msg + " to "+channel.getRemoteAddress());
                }
            }
        }
    }
    
    @Override
    public void addClient(int clientId, Channel channel) {
        // TODO Auto-generated method stub
        
    }

    private void startServer() {
        
        ConnectionlessBootstrap bootstrap = new ConnectionlessBootstrap(channelFactory);

        bootstrap.setPipelineFactory(channelPipelineFactory);

//        try {
            channel = (DatagramChannel) bootstrap.bind(new InetSocketAddress(port));
//        } catch (Exception e) {
//            channel = (DatagramChannel) bootstrap.bind(new InetSocketAddress(8081));
//        }
           System.out.println("Bound :" + channel);
    }

    private void setupConnections() throws IOException {
        ConnectionlessBootstrap bootstrap = new ConnectionlessBootstrap(new NioDatagramChannelFactory(
                Executors.newCachedThreadPool()));
//                Executors.newSingleThreadExecutor()));

        // Set up the pipeline factory.
        bootstrap.setPipelineFactory(channelPipelineFactory);

        // Start the connection attempt.
//        sendChannel = (DatagramChannel) bootstrap.bind(new InetSocketAddress(0));
        
//        sendChannel.write("hola", new InetSocketAddress("localhost", 8081));
        
        for (String server : servers) {
            // Parse options.
            final String url[] = server.split(":");
            final String hostname = url[0];
            final int port = Integer.parseInt(url[1]);
            
            DatagramChannel serverChannel = (DatagramChannel) bootstrap.bind(new InetSocketAddress(0));
            serverChannel.connect(new InetSocketAddress(hostname, port));
            serverAddresses.add(serverChannel);
        }

        int clientId = 0;
        for (String client : clients) {
            // Parse options.
            final String url[] = client.split(":");
            final String hostname = url[0];
            final int port = Integer.parseInt(url[1]);
            
            clientAddresses.put(clientId++, new InetSocketAddress(hostname, port));
        }
    }
    
    @Override
    public void close() throws IOException {
        // TODO Auto-generated method stub
        
    }
}
