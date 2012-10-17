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

package com.yahoo.pasc.paxos.server;

import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.pasc.Message;
import com.yahoo.pasc.PascRuntime;
import com.yahoo.pasc.paxos.messages.Accept;
import com.yahoo.pasc.paxos.messages.ControlMessage;
import com.yahoo.pasc.paxos.messages.DigestToSM;
import com.yahoo.pasc.paxos.messages.Execute;
import com.yahoo.pasc.paxos.messages.Hello;
import com.yahoo.pasc.paxos.messages.InvalidMessage;
import com.yahoo.pasc.paxos.messages.LeadershipChange;
import com.yahoo.pasc.paxos.messages.MessageType;
import com.yahoo.pasc.paxos.messages.PaxosMessage;
import com.yahoo.pasc.paxos.messages.PreReply;
import com.yahoo.pasc.paxos.state.PaxosState;
import com.yahoo.pasc.paxos.statemachine.ControlHandler;
import com.yahoo.pasc.paxos.statemachine.Response;
import com.yahoo.pasc.paxos.statemachine.StateMachine;

public class ServerHandler extends SimpleChannelHandler implements LeadershipObserver {

    private static final Logger LOG = LoggerFactory.getLogger(ServerHandler.class);

    private PascRuntime<PaxosState> runtime;
    private ServerConnection serverConnection;
    private boolean startedMonitor = false;
    private StateMachine stateMachine;
    private ControlHandler controlHandler;

    public ServerHandler(PascRuntime<PaxosState> runtime, StateMachine stateMachine, ControlHandler controlHandler, 
            ServerConnection serverConnection) {
        this.runtime = runtime;
        this.serverConnection = serverConnection;
        this.stateMachine = stateMachine;
        this.controlHandler = controlHandler;
    }

    @Override
    public synchronized void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        if (!startedMonitor) {
            startedMonitor = true;
//            new Thread(new ThroughputMonitor()).start();
        }
    }

    long hits[] = new long[MessageType.values().length];
    long time[] = new long[MessageType.values().length];
    long accepts = 0;
    long requests = 0;

    private BlockingQueue<Message> leadershipQueue = new LinkedBlockingQueue<Message>();

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        PaxosMessage message = (PaxosMessage) e.getMessage();
        if (LOG.isTraceEnabled()) {
            LOG.trace("[{}] Message received {}", serverConnection.getId(), message);
        }
        if (message instanceof InvalidMessage) {
            return;
        }
        if (message instanceof ControlMessage) {
            if (controlHandler != null) {
                controlHandler.handleControlMessage((ControlMessage) message);
            }
            return;
        }
        MessageType type = MessageType.getMessageType(message);
        long startTime = System.nanoTime();
        if (!leadershipQueue.isEmpty()) {
            Message leadChange = null;
            while (!leadershipQueue.isEmpty()) {
                leadChange = leadershipQueue.poll();
            }
            LOG.warn("Handling leadership change {}", leadChange);
            List<Message> toForward = runtime.handleMessage(leadChange);
            serverConnection.forward(toForward);
        }
        try {
            if (message instanceof Hello) {
                Hello r = (Hello) e.getMessage();
                serverConnection.addClient(r.getClientId(), e.getChannel());
                return;
            } else if (message instanceof Accept) {
                accepts++;
                requests += ((Accept)message).getInstance().getClientTimestamps().length;
            }
            List<Message> responses = runtime.handleMessage(message);
            if (responses == null)
                return;
            List<Message> toForward = new ArrayList<Message>();
            if (LOG.isTraceEnabled()) {
                LOG.trace("[{}] Output messages {}", serverConnection.getId(), responses);
            }
            for (Message m : responses) {
                if (m instanceof Execute) {
                    Execute execute = (Execute) m;
                    Response response = stateMachine.execute(execute);
                    PreReply rep = new PreReply(execute.getClientId(), execute.getTimestamp(), response.getResponse());
                    if (execute.isGenerateDigest()) {
                        rep.setDigest(stateMachine.digest());
                        rep.setIid(execute.getIid());
                    }
                    List<Message> replies = runtime.handleMessage(rep);
                    if (response.getAsyncMessages() != null) {
                        for (Message we : response.getAsyncMessages()) {
                            we.storeReplica(we);
                        }
                        toForward.addAll(response.getAsyncMessages());
                    }
                    if (replies != null) {
                        toForward.addAll(replies);
                    }
                } else if (m instanceof DigestToSM){
                    DigestToSM digest = (DigestToSM) m;
                    stateMachine.installDigest(digest.getDigest());
                }
                else {
                    toForward.add(m);
                }
            }
            if (LOG.isTraceEnabled()) {
                LOG.trace("[{}] Forwarding messages {}", serverConnection.getId(), toForward);
            }
            serverConnection.forward(toForward);
        } finally {
            hits[type.ordinal()]++;
            time[type.ordinal()] += System.nanoTime() - startTime;
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
        if (!(e.getCause() instanceof ClosedChannelException))
            e.getCause().printStackTrace();

        Channel ch = e.getChannel();
        ch.close();
    }

    @Override
    public void acquireLeadership() {
        leadershipQueue.add(new LeadershipChange(true));
    }

    @Override
    public void releaseLeadership() {
        leadershipQueue.add(new LeadershipChange(false));
    }

//    private class ThroughputMonitor implements Runnable {
//        @Override
//        public void run() {
//            MessageType values [] = MessageType.values();
//            while (true) {
//                try {
//                    Thread.sleep(1000);
//                } catch (InterruptedException e) {
//                    return;
//                }
//                System.out.format("Memory usage: %d bytes\n", Runtime.getRuntime().totalMemory()
//                        - Runtime.getRuntime().freeMemory());
//                StringBuilder sb = new StringBuilder();
//                for (int i = 0; i < values.length; ++i) {
//                    sb.append(values[i].name()).append('\t').append(time[i] / (double) hits[i]).append('\t');
//                }
//                System.out.println(sb);
//                System.out.println("MSGS/BATCH " + (requests / (double)accepts));
//            }
//        }
//    }
}
