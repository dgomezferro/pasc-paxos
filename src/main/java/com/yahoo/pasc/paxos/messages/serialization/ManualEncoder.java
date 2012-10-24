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

package com.yahoo.pasc.paxos.messages.serialization;

import static org.jboss.netty.channel.Channels.write;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelDownstreamHandler;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.pasc.paxos.messages.Accept;
import com.yahoo.pasc.paxos.messages.Accepted;
import com.yahoo.pasc.paxos.messages.AsyncMessage;
import com.yahoo.pasc.paxos.messages.Bye;
import com.yahoo.pasc.paxos.messages.ControlMessage;
import com.yahoo.pasc.paxos.messages.Digest;
import com.yahoo.pasc.paxos.messages.Hello;
import com.yahoo.pasc.paxos.messages.Leader;
import com.yahoo.pasc.paxos.messages.MessageType;
import com.yahoo.pasc.paxos.messages.PaxosMessage;
import com.yahoo.pasc.paxos.messages.Prepare;
import com.yahoo.pasc.paxos.messages.Prepared;
import com.yahoo.pasc.paxos.messages.Reply;
import com.yahoo.pasc.paxos.messages.Request;
import com.yahoo.pasc.paxos.messages.ServerHello;
import com.yahoo.pasc.paxos.state.ClientTimestamp;
import com.yahoo.pasc.paxos.state.InstanceRecord;

public class ManualEncoder implements ChannelDownstreamHandler {

    private static final Logger LOG = LoggerFactory.getLogger(ManualEncoder.class);

    public static int[] messages = new int[30];

    private BufferPool pool = new BufferPool();

    public BufferPool getPool() {
        return pool;
    }

    public void handleDownstream(final ChannelHandlerContext ctx, ChannelEvent evt) {
        if (!(evt instanceof MessageEvent)) {
            ctx.sendDownstream(evt);
            return;
        }

        MessageEvent e = (MessageEvent) evt;
        PaxosMessage originalMessage = (PaxosMessage) e.getMessage();

        int size = getSize(originalMessage, true);

        ChannelBuffer encodedMessage = encode(originalMessage, true, size, pool.getDirectBuffer(size));

        ChannelFuture future = e.getFuture();

        if (LOG.isTraceEnabled()) {
            LOG.trace("Encoded message with bytes {} ", ChannelBuffers.copiedBuffer(encodedMessage).array());
        }

        write(ctx, future, encodedMessage, e.getRemoteAddress());
    }

    public static int getSize(PaxosMessage msg, boolean encodeCRC) {
        MessageType type = MessageType.getMessageType(msg);
        int result = 1; // type
        if (encodeCRC) {
            result += 12; // len + CRC
        }

        switch (type) {
        case REQUEST:
        case INLINEREQ:
            Request s = (Request) msg;
            result += 16 + s.getRequest().length;
            break;
        case ACCEPT:
            Accept a = (Accept) msg;
            result += 24 + a.getArraySize() * 12;
            byte[][] requests = a.getRequests();
            if (requests != null) {
                for (byte[] request : requests) {
                    result += 4;
                    if (request != null)
                        result += request.length;
                }
            }
            break;
        case ACCEPTED:
            result += 16;
            break;
        case REPLY:
            Reply r = (Reply) msg;
            result += 20 + r.getValue().length;
            break;
        case DIGEST:
            result += 20;
            break;
        case HELLO:
            result += 4;
            break;
        case LEADER:
            result += 4;
            break;
        case SERVERHELLO:
            result += 8;
            break;
        case BYE:
            result += 8;
            break;
        case PREPARE:
            result += 16;
            break;
        case PREPARED:
            result += ((Prepared) msg).size();
            break;
        case ASYNC_MESSAGE:
            result += 20 + ((AsyncMessage) msg).getMessage().length;
            break;
        case CONTROL:
            result += 8 + ((ControlMessage) msg).getControlMessage().length;
            break;
        default:
            throw new RuntimeException("unknown size for message " + msg + " with type " + type);
        }
        return result;
    }

    public ChannelBuffer encode(PaxosMessage msg, boolean encodeCRC, int size, ChannelBuffer buffer) {
        // PaxosMessage m = (PaxosMessage) msg;
        MessageType type = MessageType.getMessageType(msg);
        // ChannelBuffer buffer = encodeCRC ? pool.getDirectBuffer() :
        // pool.getBuffer();
        // buffer.clear();
        if (encodeCRC) {
            buffer.writeInt(size);
            buffer.writeLong(msg.getCRC());
        }
        buffer.writeByte(type.ordinal());

        switch (type) {
        case REQUEST:
        case INLINEREQ: {
            Request s = (Request) msg;
            buffer.writeInt(s.getClientId());
            buffer.writeLong(s.getTimestamp());
            byte[] request = s.getRequest();
            buffer.writeInt(request.length);
            buffer.writeBytes(request);
            break;
        }
        case ACCEPT: {
            Accept a = (Accept) msg;
            // System.out.println("Encoding accept " + a);
            buffer.writeInt(a.getSenderId());
            buffer.writeInt(a.getBallot());
            buffer.writeLong(a.getIid());
            ClientTimestamp[] values = a.getValues();
            buffer.writeInt(a.getArraySize());
            for (int i = 0; i < a.getArraySize(); i++) {
                ClientTimestamp value = values[i];
                // Request valueRequest = value.getRequest();
                // System.out.println("Encoding value " + value);
                buffer.writeInt(value.getClientId());
                buffer.writeLong(value.getTimestamp());
            }
            byte[][] requests = a.getRequests();
            if (requests == null) {
                buffer.writeInt(-1);
            } else {
                buffer.writeInt(requests.length);
                for (byte[] request : requests) {
                    if (request == null) {
                        buffer.writeInt(-1);
                    } else {
                        buffer.writeInt(request.length);
                        buffer.writeBytes(request);
                    }
                }
            }
            break;
        }
        case ACCEPTED: {
            Accepted ad = (Accepted) msg;
            buffer.writeInt(ad.getSenderId());
            buffer.writeInt(ad.getBallot());
            buffer.writeLong(ad.getIid());
            break;
        }
        case REPLY: {
            Reply r = (Reply) msg;
            buffer.writeInt(r.getServerId());
            buffer.writeInt(r.getClientId());
            buffer.writeLong(r.getTimestamp());
            byte[] v = r.getValue();
            buffer.writeInt(v.length);
            buffer.writeBytes(v);
            break;
        }
        case DIGEST: {
            Digest d = (Digest) msg;
            buffer.writeInt(d.getSenderId());
            buffer.writeLong(d.getDigestId());
            buffer.writeLong(d.getDigest());
            break;
        }
        case HELLO: {
            Hello h = (Hello) msg;
            buffer.writeInt(h.getClientId());
            break;
        }
        case LEADER: {
            Leader l = (Leader) msg;
            buffer.writeInt(l.getLeader());
            break;
        }
        case SERVERHELLO: {
            ServerHello h = (ServerHello) msg;
            buffer.writeInt(h.getClientId());
            buffer.writeInt(h.getServerId());
            break;
        }
        case BYE: {
            Bye b = (Bye) msg;
            buffer.writeInt(b.getClientId());
            buffer.writeInt(b.getServerId());
            break;
        }
        case PREPARE: {
            Prepare p = (Prepare) msg;
            buffer.writeInt(p.getSenderId());
            buffer.writeInt(p.getBallot());
            buffer.writeLong(p.getMaxExecutedIid());
            break;
        }
        case PREPARED: {
            Prepared pd = (Prepared) msg;
            buffer.writeInt(pd.getSenderId());
            buffer.writeInt(pd.getReceiver());
            buffer.writeInt(pd.getReplyBallot());

            buffer.writeInt(pd.getAcceptedSize());
            InstanceRecord[] accepted = pd.getAcceptedReqs();
            for (int i = 0; i < pd.getAcceptedSize(); i++) {
                InstanceRecord currAccepted = accepted[i];
                buffer.writeLong(currAccepted.getIid());
                buffer.writeInt(currAccepted.getBallot());
                buffer.writeInt(currAccepted.getArraySize());
                ClientTimestamp[] ct = currAccepted.getClientTimestamps();
                for (int j = 0; j < currAccepted.getArraySize(); j++) {
                    buffer.writeInt(ct[j].getClientId());
                    buffer.writeLong(ct[j].getTimestamp());
                }
            }

            buffer.writeInt(pd.getLearnedSize());
            Accept[] learned = pd.getLearnedReqs();
            for (int i = 0; i < pd.getLearnedSize(); i++) {
                Accept currLearned = learned[i];
                encode(currLearned, true, ManualEncoder.getSize(currLearned, true), buffer);
            }

            buffer.writeInt(pd.getCheckpointDigest().getDigestId());
            buffer.writeLong(pd.getCheckpointDigest().getDigest());

            buffer.writeLong(pd.getMaxForgotten());
            break;
        }
        case ASYNC_MESSAGE: {
            AsyncMessage am = (AsyncMessage) msg;
            buffer.writeInt(am.getClientId());
            buffer.writeInt(am.getServerId());
            buffer.writeLong(am.getTimestamp());
            buffer.writeInt(am.getMessage().length);
            buffer.writeBytes(am.getMessage());
            break;
        }
        case CONTROL: {
            ControlMessage cm = (ControlMessage) msg;
            buffer.writeInt(cm.getClientId());
            buffer.writeInt(cm.getControlMessage().length);
            buffer.writeBytes(cm.getControlMessage());
            break;
        }
        default:
            throw new UnsupportedOperationException("Unknown message type " + type);
        }

        return buffer;
    }
}
