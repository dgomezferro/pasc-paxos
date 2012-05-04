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
import java.util.Arrays;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.zookeeper.KeeperException;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;

import com.yahoo.pasc.Message;
import com.yahoo.pasc.PascRuntime;
import com.yahoo.pasc.paxos.client.messages.Received;
import com.yahoo.pasc.paxos.client.messages.Submit;
import com.yahoo.pasc.paxos.client.messages.Timeout;
import com.yahoo.pasc.paxos.messages.Hello;
import com.yahoo.pasc.paxos.messages.InlineRequest;
import com.yahoo.pasc.paxos.messages.Request;

public class PaxosClientHandler extends SimpleChannelUpstreamHandler implements PaxosInterface {

//    private static final Logger LOG = LoggerFactory.getLogger(PaxosClientHandler.class);

    private int clientId;
    private int clients;
    private int timeout;
    private String zkConnection;
    private int warmup = 20000;
    private long measuringTime = 30000;
    private ChannelGroup serverChannels = new DefaultChannelGroup("Servers");
    private Channel leader = null;
    private PascRuntime<ClientState> runtime;
    private ClientInterface clientInterface;

    /**
     * Creates a client-side handler.
     */
    public PaxosClientHandler(PascRuntime<ClientState> runtime, ClientInterface clientInterface, int clientId, int clients,
            int timeout, String zkConnection) {
        this.clientId = clientId;
        this.clients = clients;
        this.timeout = timeout;
        this.zkConnection = zkConnection;
        this.runtime = runtime;
        this.clientInterface = clientInterface;
        this.clientInterface.setInterface(this);
        Arrays.fill(payload, (byte)5);
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
    public void submitNewRequest(byte[] request) {
        if (resubmit != null) resubmit.cancel();
        List<Message> messages = runtime.handleMessage(new Submit(request));
        if (messages == null)
            return;
        for (Message m : messages) {
//            LOG.trace("Sending {}", m);
            if (m instanceof InlineRequest) {
                leader.write(m);
            } else {
                serverChannels.write(m);
            }
            pendingSentTime = System.nanoTime();
//            System.out.println("NOT RESUBMITTING");
            resubmit = new ResubmitTask(((Request) m).getTimestamp());
            timer.schedule(resubmit, timeout);
        }
    }
    
    synchronized void resubmitRequest(long timestamp) {
        List<Message> messages = runtime.handleMessage(new Timeout(timestamp));
        if (messages == null)
            return;
        for (Message m : messages) {
//            LOG.trace("Sending {}", m);
            resubmits++;
            serverChannels.write(m);
            resubmit = new ResubmitTask(timestamp);
            timer.schedule(resubmit, timeout);
        }
    }

    @Override
    public synchronized void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) {
        Hello hello = new Hello(clientId);
        hello.storeReplica(hello);
        e.getChannel().write(hello);
    }

    private long messagesReceived = 0;
    private long lastReceived = 0;
    private long lastTime = 0;
    
    @Override
    public synchronized void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
        Object message = e.getMessage();
        if (message instanceof Hello) {
            Hello hello = (Hello) e.getMessage();
            if (hello.getClientId() == 0) {
                System.out.println("Setting leader to " + e.getChannel());
                leader = e.getChannel();
            }
            serverChannels.add(e.getChannel());
        }
//        LOG.trace("Received {}", message);
        List<Message> messages = runtime.handleMessage((Message) message);
        if (messages == null)
            return;
        for (Message m : messages) {
//            LOG.trace("Sending {}", m);
            if (m instanceof Connected) {
                lastTime = System.nanoTime();
                
                if (zkConnection != null) {
                    new Thread(new ThroughputMonitor()).start();
                }

                clientInterface.connected();
            } else if (m instanceof Received) {
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
                
                clientInterface.messageReceived(((Received)m).getReply());
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
        System.err.println("Unexpected exception from downstream: " + e.getCause());
        e.getCause().printStackTrace();
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

    class ThroughputMonitor implements Runnable {
        private Barrier barrier;
        public ThroughputMonitor() {
            try {
                System.out.println("Size "+clients);
                barrier = new Barrier(zkConnection, "/paxos", ""+clientId, clients);
            } catch (KeeperException e) {
//                LOG.error("Couldnt create barrier", e);
                System.out.println("Couldnt create barrier"); e.printStackTrace();
            } catch (IOException e) {
//                LOG.error("Couldnt create barrier", e);
                System.out.println("Couldnt create barrier"); e.printStackTrace();
            }
        }
        
        @Override
        public void run() {
            try {
                try {
                    Thread.sleep(warmup);
                } catch (InterruptedException ignore) {
                }
//                System.out.format("[%d] Entering barrier\n", clientId);
                barrier.enter();
//                System.out.format("[%d] Entered barrier\n", clientId);
                long startMessagges = messagesReceived;
                long startTime = System.currentTimeMillis();
                measureLatency = true;
                try {
                    Thread.sleep(getMeasuringTime());
                } catch (InterruptedException ignore) {
                }
//                System.out.format("[%d] Leaving barrier\n", clientId);
                barrier.leave();
                System.out.println(String.format("Throughput: %8.8f m/s", 
                        (messagesReceived - startMessagges) / ((double)(System.currentTimeMillis() - startTime)/1000)));
                System.out.println(String.format("Latency: %4.4f ms", 
                        (totalTime / (double) latencyReceived)/1000000));
                System.out.flush();
                
                barrier.close();
                
                serverChannels.close();

                Thread.sleep(2000);

                System.exit(0);
            } catch (KeeperException e) {
                System.out.println("Couldnt measure throughput"); e.printStackTrace();
            } catch (InterruptedException e) {
                System.out.println("Couldnt measure throughput"); e.printStackTrace();
            }
            System.exit(-1);
        }
    }
}
