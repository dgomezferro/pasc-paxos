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

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.execution.ExecutionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.pasc.Message;
import com.yahoo.pasc.PascRuntime;
import com.yahoo.pasc.paxos.Barrier;
import com.yahoo.pasc.paxos.client.messages.Received;
import com.yahoo.pasc.paxos.client.messages.Submit;
import com.yahoo.pasc.paxos.client.messages.Timeout;
import com.yahoo.pasc.paxos.messages.ControlMessage;
import com.yahoo.pasc.paxos.messages.Hello;
import com.yahoo.pasc.paxos.messages.InlineRequest;
import com.yahoo.pasc.paxos.messages.InvalidMessage;
import com.yahoo.pasc.paxos.messages.Leader;
import com.yahoo.pasc.paxos.messages.Request;
import com.yahoo.pasc.paxos.messages.ServerHello;
import com.yahoo.pasc.paxos.messages.serialization.ManualDecoder;
import com.yahoo.pasc.paxos.messages.serialization.ManualEncoder;

public class PaxosClientHandler extends SimpleChannelUpstreamHandler implements PaxosInterface {

    private static final Logger LOG = LoggerFactory.getLogger(PaxosClientHandler.class);
    private static final int MAX_CLIENTS = 4096;

    private int clientId;
    private int clients;
    private int timeout;
    private int warmup = 20000;
    private long measuringTime = 30000;
    private String[] servers;
    private volatile Channel[] serverChannels;
    private volatile int leader = 0;
    private PascRuntime<ClientState> runtime;
    private ClientInterface clientInterface;
    private ChannelFactory channelFactory = new NioClientSocketChannelFactory(Executors.newCachedThreadPool(),
            Executors.newCachedThreadPool());
    private ChannelPipelineFactory channelPipelineFactory;

    /**
     * Creates a client-side handler.
     * 
     * @throws IOException
     */
    public PaxosClientHandler(final PascRuntime<ClientState> runtime, ClientInterface clientInterface, String[] servers,
            int clients, int timeout, String zkConnection, final ExecutionHandler executor)
            throws IOException {
        this.clients = clients;
        this.timeout = timeout;
        this.runtime = runtime;
        this.clientInterface = clientInterface;
        this.clientInterface.setInterface(this);
        this.channelPipelineFactory = new ChannelPipelineFactory() {
            public ChannelPipeline getPipeline() throws Exception {
                return Channels.pipeline(new ManualEncoder(), new ManualDecoder(runtime.isProtected()), executor,
                        PaxosClientHandler.this);
            }
        };
        this.servers = servers;
        this.serverChannels = new Channel[servers.length];
        Arrays.fill(payload, (byte) 5);
        generateClientId();
        for (int i = 0; i < servers.length; ++i) {
            tryConnect(i);
        }
    }

    private int period = 100;
    private byte[] payload = new byte[1];
    private int resubmits;
    private Timer timer = new Timer("Resubmit Thread");
    private TimerTask resubmit;
    private boolean measureLatency = false;
    private long totalTime = 0;
    private long pendingSentTime = 0;
    private int latencyReceived = 0;

    class ResubmitTask extends TimerTask {
        long timestamp;

        public ResubmitTask(long timestamp) {
            this.timestamp = timestamp;
        }

        @Override
        public void run() {
            resubmitRequest(timestamp);
        }
    }

    @Override
    public synchronized void submitNewRequest(byte[] request) {
        if (resubmit != null)
            resubmit.cancel();
        List<Message> messages = runtime.handleMessage(new Submit(request));
        if (messages == null)
            return;
        for (Message m : messages) {
            LOG.trace("Sending {}", m);
            if (m instanceof InlineRequest) {
                send(m);
            } else {
                sendAll(m);
            }
            pendingSentTime = System.nanoTime();
            // System.out.println("NOT RESUBMITTING");
            resubmit = new ResubmitTask(((Request) m).getTimestamp());
            timer.schedule(resubmit, timeout);
        }
    }

    public void close() {
        if (timer != null)
            timer.cancel();
        if (resubmit != null)
            resubmit.cancel();
        for (Channel c : serverChannels) {
            if (c != null) {
                c.close();
            }
        }
        if (connectionThread != null) {
            connectionThread.interrupt();
        }
    }

    @Override
    public synchronized void submitControlMessage(byte[] controlMessage) {
        ControlMessage cm = new ControlMessage(clientId, controlMessage);
        cm.storeReplica(cm);
        sendAll(cm);
    }

    private static final String ELECTION_PATH = "/pasc_election";
    private Thread connectionThread;

    public void start() throws KeeperException, InterruptedException {
        connectionThread = new Thread(new ConnectionThread());
        connectionThread.start();
    }

    private void send(Message m) {
        if (leader == -1) {
            sendAll(m);
            return;
        }

        Channel channel = serverChannels[leader];
        if (channel != null && channel.isConnected()) {
            channel.write(m);
        } else {
            sendAll(m);
        }
    }

    private void sendAll(Message m) {
        for (Channel channel : serverChannels) {
            if (channel != null && channel.isConnected()) {
                channel.write(m);
            }
        }
    }

    synchronized void resubmitRequest(long timestamp) {
        List<Message> messages = runtime.handleMessage(new Timeout(timestamp));
        if (messages == null)
            return;
        for (Message m : messages) {
            LOG.trace("Sending {}", m);
            resubmits++;
            sendAll(m);
            resubmit = new ResubmitTask(timestamp);
            timer.schedule(resubmit, timeout);
        }
    }
    
    private Random random = new Random();

    private void generateClientId() {
        int newId = random.nextInt(MAX_CLIENTS);
        LOG.warn("Previous id " + clientId + " newId " + newId);
        clientId = newId;
    }
    
    private void sendHello(Channel c) {
        Hello hello = new Hello(clientId);
        hello.storeReplica(hello);
        runtime.handleMessage(hello);
        c.write(hello);
    }

    @Override
    public synchronized void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) {
        sendHello(e.getChannel());
    }

    private long messagesReceived = 0;
    private long lastReceived = 0;
    private long lastTime = 0;

    @Override
    public synchronized void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
        Object message = e.getMessage();
        if (message instanceof InvalidMessage) {
            return;
        }
        if (message instanceof Leader) {
            Leader l = (Leader) e.getMessage();
            int newLeader = l.getLeader();
            if (newLeader >= 0 && newLeader < servers.length) {
                leader = newLeader;
            } else {
                leader = -1;
            }
            return;
        }
        if (message instanceof ServerHello) {
            ServerHello hello = (ServerHello) e.getMessage();
            serverChannels[hello.getServerId()] = e.getChannel();
            LOG.debug("Received hello: " + message);
        }
        LOG.trace("Received {}", message);
        List<Message> messages = runtime.handleMessage((Message) message);
        if (messages == null)
            return;
        for (Message m : messages) {
            if (m instanceof Connected) {
                lastTime = System.nanoTime();

                // new Thread(new ThroughputMonitor()).start();

                clientInterface.connected();
            } else if (m instanceof Reconnect) {
                generateClientId();
                for (Channel c : serverChannels) {
                    if (c != null)
                        sendHello(c);
                }
            } else if (m instanceof Received) {
                if (resubmit != null)
                    resubmit.cancel();
                ++messagesReceived;
                if (messagesReceived % period == 0) {
                    long currentTime = System.nanoTime();
                    long elapsed = currentTime - lastTime;
                    System.out.println(String.format("%8d: %8.2f m/s Resubmits: %d", messagesReceived,
                            (messagesReceived - lastReceived) / ((double) elapsed / 1000000000), resubmits));
                    resubmits = 0;
                    lastTime = currentTime;
                    lastReceived = messagesReceived;
                }
                if (measureLatency) {
                    totalTime += System.nanoTime() - pendingSentTime;
                    latencyReceived++;
                }

                clientInterface.messageReceived(((Received) m).getReply());
            }
        }
    }

    @Override
    public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        super.channelDisconnected(ctx, e);
        Channel channel = e.getChannel();
        for (int i = 0; i < serverChannels.length; ++i) {
            if (channel.equals(serverChannels[i])) {
                serverChannels[i].close();
                serverChannels[i] = null;
                tryConnect(i);
            }
        }
    }

    private BlockingQueue<Integer> toConnect = new LinkedBlockingQueue<Integer>();

    private void tryConnect(final int i) {
        toConnect.add(i);
    }

    private class ConnectionThread implements Runnable, Closeable {
        private final int MAX_SEQ_ATTEMPTS = 3;
        private volatile boolean exit = false;
        private int attempts = 0;
        private int timeout = 1000;
        private Set<ClientBootstrap> bootstraps = new HashSet<ClientBootstrap>();

        @Override
        public void run() {
            try {
                while (!exit) {
                    if (attempts > MAX_SEQ_ATTEMPTS) {
                        attempts = 0;
                        Thread.sleep(timeout);
                        timeout *= 2;
                    }
                    attempts++;

                    final int id = toConnect.take();

                    ClientBootstrap bootstrap = new ClientBootstrap(channelFactory);
                    bootstraps.add(bootstrap);

                    // Set up the pipeline factory.
                    bootstrap.setPipelineFactory(channelPipelineFactory);
                    bootstrap.setOption("tcpNoDelay", true);
                    bootstrap.setOption("keepAlive", true);

                    String server = servers[id];
                    final String s[] = server.split(":");
                    final String hostname = s[0];
                    final int port = Integer.parseInt(s[1]);

                    // Start the connection attempt.
                    ChannelFuture connection = bootstrap.connect(new InetSocketAddress(hostname, port));
                    connection.addListener(new ChannelFutureListener() {
                        @Override
                        public void operationComplete(ChannelFuture future) throws Exception {
                            if (future.isSuccess()) {
                                serverChannels[id] = future.getChannel();
                            } else {
                                tryConnect(id);
                            }
                        }
                    });
                }
            } catch (InterruptedException e) {
                close();
            }
        }

        @Override
        public void close() {
            exit = true;
            for (ClientBootstrap cb : bootstraps) {
                cb.releaseExternalResources();
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
        LOG.warn("Unexpected exception: " + e.getCause());
        e.getChannel().close();
    }

    public int getWarmup() {
        return warmup;
    }

    public void setWarmup(int warmup) {
        this.warmup = warmup;
    }

    public void setMeasuringTime(long measuringTime) {
        this.measuringTime = measuringTime;
    }

    public long getMeasuringTime() {
        return measuringTime;
    }

    public void setRequestSize(int size) {
        payload = new byte[size];
        Arrays.fill(payload, (byte) 6);
    }

    public int getPeriod() {
        return period;
    }

    public void setPeriod(int period) {
        this.period = period;
    }

}
