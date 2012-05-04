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
import java.util.Arrays;
import java.util.List;

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
import com.yahoo.pasc.paxos.messages.Execute;
import com.yahoo.pasc.paxos.messages.Hello;
import com.yahoo.pasc.paxos.messages.InlineRequest;
import com.yahoo.pasc.paxos.messages.MessageType;
import com.yahoo.pasc.paxos.messages.PaxosMessage;
import com.yahoo.pasc.paxos.messages.PreReply;
import com.yahoo.pasc.paxos.state.PaxosState;
import com.yahoo.pasc.paxos.statemachine.StateMachine;

public class ServerHandler extends SimpleChannelHandler {

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(ServerHandler.class);

    private PascRuntime<PaxosState> runtime;
    private ServerConnection serverConnection;
    private boolean startedMonitor = false;
    private StateMachine stateMachine;

    public ServerHandler(PascRuntime<PaxosState> runtime, StateMachine stateMachine, ServerConnection serverConnection) {
        this.runtime = runtime;
        this.serverConnection = serverConnection;
        this.stateMachine = stateMachine;
    }

    @Override
    public synchronized void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        if (!startedMonitor) {
            startedMonitor = true;
            new Thread(new ThroughputMonitor()).start();
        }
    }

    long hits[] = new long[MessageType.values().length];
    long time[] = new long[MessageType.values().length];
    long accepts = 0;
    long requests = 0;

    private boolean leader = false;

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        PaxosMessage message = (PaxosMessage) e.getMessage();
//        LOG.debug("Message received {}", message);
        MessageType type = MessageType.getMessageType(message);
        long startTime = System.nanoTime();
        try {
            if (message instanceof Hello) {
                Hello r = (Hello) e.getMessage();
                serverConnection.addClient(r.getClientId(), e.getChannel());
                serverConnection.forward(Arrays.<Message> asList(r));
                return;
            } else if (message instanceof Accept) {
                accepts++;
                requests += ((Accept)message).getInstance().getClientTimestamps().length;
                if (leader) return;
            } else if (message instanceof InlineRequest){
                leader  = true;
            }
//             LOG.trace("Received: {} ", e.getMessage());
            List<Message> responses = runtime.handleMessage(message);
            if (responses == null)
                return;
            List<Message> toForward = new ArrayList<Message>();
            for (Message m : responses) {
                if (m instanceof Execute) {
                    Execute execute = (Execute) m;
                    byte[] response = stateMachine.execute(execute.getRequest());
                    PreReply rep = new PreReply(execute.getClientId(), execute.getTimestamp(), response);
                    if (execute.isGenerateDigest()) {
                        rep.setDigest(stateMachine.digest());
                        rep.setIid(execute.getIid());
                    }
                    List<Message> replies = runtime.handleMessage(rep);
                    if (replies != null) {
                        toForward.addAll(replies);
                    }
                } else {
                    toForward.add(m);
                }
            }
//            LOG.trace("Forwarding: {} ", toForward);
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

    private class ThroughputMonitor implements Runnable {
        @Override
        public void run() {
            MessageType values [] = MessageType.values();
            while (true) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    return;
                }
                System.out.format("Memory usage: %d bytes\n", Runtime.getRuntime().totalMemory()
                        - Runtime.getRuntime().freeMemory());
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < values.length; ++i) {
                    sb.append(values[i].name()).append('\t').append(time[i] / (double) hits[i]).append('\t');
                }
                System.out.println(sb);
                System.out.println("MSGS/BATCH " + (requests / (double)accepts));
            }
        }
    }
}
