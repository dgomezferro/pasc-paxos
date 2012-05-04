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

package com.yahoo.pasc.paxos.server.tcp;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.embedder.EncoderEmbedder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.pasc.Message;
import com.yahoo.pasc.PascRuntime;
import com.yahoo.pasc.paxos.messages.Hello;
import com.yahoo.pasc.paxos.messages.Reply;
import com.yahoo.pasc.paxos.messages.serialization.ManualEncoder;
import com.yahoo.pasc.paxos.server.PipelineFactory;
import com.yahoo.pasc.paxos.server.ServerConnection;
import com.yahoo.pasc.paxos.state.PaxosState;
import com.yahoo.pasc.paxos.statemachine.StateMachine;

public class TcpServer implements ServerConnection {
    
    private static final Logger LOG = LoggerFactory.getLogger(TcpServer.class);

    private String servers[];

    private ChannelPipelineFactory channelPipelineFactory;
    private ExecutorService bossExecutor;
    private ExecutorService workerExecutor;

    private ChannelGroup serverChannels = new DefaultChannelGroup("serversTcp");
    private Map<Integer, Channel> clientChannels = new ConcurrentHashMap<Integer, Channel>(1024, 0.75f, 32);

    private int port;
    private int threads;
    private int id;
    
    private EncoderEmbedder<ChannelBuffer> embedder = new EncoderEmbedder<ChannelBuffer>(new ManualEncoder());

    private Channel serverChannel;

    public TcpServer(PascRuntime<PaxosState> runtime, StateMachine sm, String servers[], String clients[], int port,
            int threads, int id, boolean twoStages) {
        this.bossExecutor = Executors.newCachedThreadPool();
        this.workerExecutor = Executors.newCachedThreadPool();
//        this.workerExecutor = Executors.newSingleThreadExecutor();
        this.channelPipelineFactory = new PipelineFactory(runtime, sm, this, threads, id, twoStages);
        this.servers = servers;
        this.port = port;
        this.threads = threads;
        this.id = id;
    }

    public void run() {
        startServer();
        try {
            Thread.sleep(5000);
        } catch (InterruptedException ignore) {
        }
        setupConnections();
    }

    @Override
    public void forward(List<Message> messages) {
//        LOG.trace("Sending: {}", messages);
        
        if (messages == null) {
            return;
        }

        for (Message msg : messages) {
            if (msg == null) {
                continue;
            }
            
            if (msg instanceof Reply) {
                Reply reply = (Reply) msg;
                int clientId = reply.getClientId();
                Channel clientChannel = clientChannels.get(clientId);
                if (clientChannel == null) {
                    LOG.error("Client {} not yet connected. Cannot send reply.", clientId);
                    continue;
                }
//                LOG.trace("Sending {} to {}.", msg, clientChannel);
                embedder.offer(msg);
                ChannelBuffer encoded = embedder.poll();
                clientChannel.write(encoded);
            } else if (msg instanceof Hello) {
                Hello hello = (Hello) msg;
                int clientId = hello.getClientId();
                hello.setClientId(id);
                hello.storeReplica(hello);
                Channel clientChannel = clientChannels.get(clientId);
                if (clientChannel == null) {
                    LOG.error("Client {} not yet connected. Cannot send reply.", clientId);
                    continue;
                }
//                LOG.trace("Sending {} to {}.", msg, clientChannel);
                embedder.offer(hello);
                ChannelBuffer encoded = embedder.poll();
                clientChannel.write(encoded);
            } else {
                // Send to all servers
//                LOG.trace("Sending {} to {}.", msg, serverChannels);
                embedder.offer(msg);
                ChannelBuffer encoded = embedder.poll();
                serverChannels.write(encoded);
            }
            
        }
    }
    
    @Override
    public void addClient(int clientId, Channel channel) {
        if (!clientChannels.containsKey(clientId)) {
            LOG.debug("Adding client " + clientId + " " + channel);
            clientChannels.put(clientId, channel);
        }
    }

    private void startServer() {
        ServerBootstrap bootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(bossExecutor, workerExecutor, threads));

        bootstrap.setPipelineFactory(channelPipelineFactory);

        bootstrap.setOption("child.tcpNoDelay", true);
        bootstrap.setOption("child.keepAlive", true);

        serverChannel = bootstrap.bind(new InetSocketAddress(port));
        try {
            System.out.println("Bound :" + serverChannel + " at " + InetAddress.getLocalHost().getHostName());
        } catch (UnknownHostException e) {
            //ignore
        }
    }

    private void setupConnections() {
        for (String server : servers) {
            // Parse options.
            final String url[] = server.split(":");
            final String hostname = url[0];
            final int port = Integer.parseInt(url[1]);
            // Configure the client.
            ClientBootstrap bootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(bossExecutor,
                    workerExecutor));

            // Set up the pipeline factory.
            bootstrap.setPipelineFactory(channelPipelineFactory);
            bootstrap.setOption("tcpNoDelay", true);
            bootstrap.setOption("keepAlive", true);

            // Start the connection attempt.
            ChannelFuture future = null;
            while (future == null || !future.isSuccess()) {
                try {
                    if (future != null) {
                        future.cancel();
                    }
//                    Thread.sleep(1000);
                    LOG.trace("Connecting to {}:{}", hostname, port);
                    future = bootstrap.connect(new InetSocketAddress(hostname, port));
                    future.awaitUninterruptibly();
                } catch (Exception e) {
                    LOG.trace("Error during connection");
                }
            }
            serverChannels.add(future.getChannel());
        }

    }
    
    @Override
    public void close() throws IOException {
        serverChannel.close();
        serverChannels.close();
    }
}
