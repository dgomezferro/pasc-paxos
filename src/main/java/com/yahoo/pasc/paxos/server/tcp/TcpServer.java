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
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
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
import org.jboss.netty.handler.execution.ExecutionHandler;
import org.jboss.netty.handler.execution.MemoryAwareThreadPoolExecutor;
import org.jboss.netty.util.ObjectSizeEstimator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.pasc.Message;
import com.yahoo.pasc.PascRuntime;
import com.yahoo.pasc.paxos.Barrier;
import com.yahoo.pasc.paxos.messages.AsyncMessage;
import com.yahoo.pasc.paxos.messages.Bye;
import com.yahoo.pasc.paxos.messages.Leader;
import com.yahoo.pasc.paxos.messages.Prepared;
import com.yahoo.pasc.paxos.messages.Reply;
import com.yahoo.pasc.paxos.messages.ServerHello;
import com.yahoo.pasc.paxos.messages.serialization.ManualEncoder;
import com.yahoo.pasc.paxos.server.LeaderElection;
import com.yahoo.pasc.paxos.server.PipelineFactory;
import com.yahoo.pasc.paxos.server.ServerConnection;
import com.yahoo.pasc.paxos.server.ServerHandler;
import com.yahoo.pasc.paxos.state.PaxosState;
import com.yahoo.pasc.paxos.statemachine.ControlHandler;
import com.yahoo.pasc.paxos.statemachine.StateMachine;

public class TcpServer implements ServerConnection {

    private static final Logger LOG = LoggerFactory.getLogger(TcpServer.class);

    private String servers[];

    private ChannelPipelineFactory channelPipelineFactory;
    private ExecutorService bossExecutor;
    private ExecutorService workerExecutor;

    private ChannelGroup serverChannels = new DefaultChannelGroup("servers");
    private ConcurrentMap<Integer, Channel> indexedServerChannels = new ConcurrentHashMap<Integer, Channel>(1024,
            0.75f, 32);
    private ConcurrentMap<Integer, Channel> clientChannels = new ConcurrentHashMap<Integer, Channel>(1024, 0.75f, 32);

    private int port;
    private int threads;
    private int id;

    private EncoderEmbedder<ChannelBuffer> embedder = new EncoderEmbedder<ChannelBuffer>(new ManualEncoder());

    private Channel serverChannel;

    private ExecutionHandler executionHandler;

    private ServerHandler channelHandler;

    private LeaderElection leaderElection;

    private Barrier barrier;

    public TcpServer(PascRuntime<PaxosState> runtime, StateMachine sm, ControlHandler controlHandler, String zk,
            String servers[], int port, int threads, final int id, boolean twoStages) throws IOException,
            KeeperException {
        this.bossExecutor = Executors.newCachedThreadPool();
        this.workerExecutor = Executors.newCachedThreadPool();
        this.executionHandler = new ExecutionHandler(new MemoryAwareThreadPoolExecutor(1, 1024 * 1024,
                1024 * 1024 * 1024, 10, TimeUnit.SECONDS, new ObjectSizeEstimator() {
                    @Override
                    public int estimateSize(Object o) {
                        return 1024;
                    }
                }, new ThreadFactory() {
                    private int count = 0;

                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, id + "-" + count++);
                    }
                }));
        this.channelHandler = new ServerHandler(runtime, sm, controlHandler, this);
        this.channelPipelineFactory = new PipelineFactory(channelHandler, executionHandler, twoStages, runtime.isProtected());
        final CountDownLatch latch = new CountDownLatch(1);
        ZooKeeper zookeeper = new ZooKeeper(zk, 2000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                latch.countDown();
            }
        });
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        this.leaderElection = new LeaderElection(zookeeper, id, this.channelHandler);
        this.barrier = new Barrier(zookeeper, "/paxos_srv_barrier", "" + id, servers.length);
        this.servers = servers;
        this.port = port;
        this.threads = threads;
        this.id = id;
    }

    public void run() {
        startServer();
        leaderElection.start();
        try {
            barrier.enter();
            leaderElection.refresh();
            barrier.leave();
        } catch (KeeperException e) {
            LOG.error("Couldn't initialize leaderElection", e);
        } catch (InterruptedException e) {
            LOG.error("Couldn't initialize leaderElection", e);
        }
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
                int clientId = reply.getClientId();
                Channel clientChannel = clientChannels.get(clientId);
                if (clientChannel == null) {
                    LOG.error("Client {} not yet connected. Cannot send reply.", clientId);
                    continue;
                }
                embedder.offer(msg);
                ChannelBuffer encoded = embedder.poll();
                clientChannel.write(encoded);
            } else if (msg instanceof AsyncMessage) {
                AsyncMessage asyncMessage = (AsyncMessage) msg;
                int clientId = asyncMessage.getClientId();
                Channel clientChannel = clientChannels.get(clientId);
                if (clientChannel == null) {
                    LOG.error("Client {} not yet connected. Cannot send async message.", clientId);
                    continue;
                }
                embedder.offer(asyncMessage);
                ChannelBuffer encoded = embedder.poll();
                clientChannel.write(encoded);
            } else if (msg instanceof ServerHello) {
                ServerHello hello = (ServerHello) msg;
                int clientId = hello.getClientId();
                Channel clientChannel = clientChannels.get(clientId);
                if (clientChannel == null) {
                    LOG.error("Client {} not yet connected. Cannot send reply.", clientId);
                    continue;
                }
                embedder.offer(hello);
                ChannelBuffer encoded = embedder.poll();
                clientChannel.write(encoded);
            } else if (msg instanceof Bye) {
                Bye bye = (Bye) msg;
                int clientId = bye.getClientId();
                Channel clientChannel = clientChannels.get(clientId);
                if (clientChannel == null) {
                    LOG.error("Client {} not yet connected. Cannot send reply.", clientId);
                    continue;
                }
                embedder.offer(bye);
                ChannelBuffer encoded = embedder.poll();
                clientChannel.write(encoded);
            } else if (msg instanceof Prepared) {
                Prepared prepared = (Prepared) msg;
                int receiver = prepared.getReceiver();
                Channel channel = indexedServerChannels.get(receiver);
                if (channel != null) {
                    embedder.offer(msg);
                    channel.write(embedder.poll());
                    LOG.trace("Sent {} to {}.", msg, receiver);
                } else {
                    LOG.error("Server {} not yet connected. Cannot send prepared.", receiver);
                }
            } else if (msg instanceof Leader) {
                embedder.offer(msg);
                ChannelBuffer buf = embedder.poll();
                for (Channel c : clientChannels.values()) {
                    if (c != null) {
                        c.write(buf);
                    }
                }
            } else {
                embedder.offer(msg);
                ChannelBuffer encoded = embedder.poll();
                serverChannels.write(encoded);
            }

        }
    }

    @Override
    public void addClient(int clientId, Channel channel) {
        if (clientChannels.containsKey(clientId)) {
            Bye bye = new Bye(clientId, id);
            bye.storeReplica(bye);
            embedder.offer(bye);
            ChannelBuffer encoded = embedder.poll();
            channel.write(encoded);
        } else {
            LOG.debug("Adding client " + clientId + " " + channel);
            clientChannels.put(clientId, channel);
            ServerHello sh = new ServerHello(clientId, id);
            sh.storeReplica(sh);
            forward(Arrays.<Message> asList(sh));
        }
    }

    private void startServer() {
        ServerBootstrap bootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(bossExecutor, workerExecutor,
                threads));

        bootstrap.setPipelineFactory(channelPipelineFactory);

        bootstrap.setOption("child.tcpNoDelay", true);
        bootstrap.setOption("child.keepAlive", true);

        serverChannel = bootstrap.bind(new InetSocketAddress(port));
        try {
            LOG.warn("Bound :" + serverChannel + " at " + InetAddress.getLocalHost().getHostName());
        } catch (UnknownHostException e) {
            // ignore
        }
    }

    private void setupConnections() {
        int id = 0;
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
            LOG.trace("Connecting to {}:{}", hostname, port);
            ChannelFuture future = bootstrap.connect(new InetSocketAddress(hostname, port));
            future.awaitUninterruptibly();
            long wait = 1000;
            while (!future.isSuccess()) {
                try {
                    future.cancel();
                    Thread.sleep(wait);
                    wait *= 2;
                    future = bootstrap.connect(new InetSocketAddress(hostname, port));
                    future.awaitUninterruptibly();
                } catch (Exception e) {
                    LOG.trace("Error during connection");
                }
            }
            serverChannels.add(future.getChannel());
            indexedServerChannels.put(id, future.getChannel());
            id++;
        }

    }

    @Override
    public void close() throws IOException {
        serverChannel.close();
        serverChannels.close();
        indexedServerChannels.clear();
    }

    @Override
    public int getId() {
        return id;
    }
}
