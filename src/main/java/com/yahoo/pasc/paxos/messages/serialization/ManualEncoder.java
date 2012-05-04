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
import org.jboss.netty.channel.ChannelDownstreamHandler;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.pasc.paxos.messages.Accept;
import com.yahoo.pasc.paxos.messages.Accepted;
import com.yahoo.pasc.paxos.messages.Digest;
import com.yahoo.pasc.paxos.messages.Hello;
import com.yahoo.pasc.paxos.messages.MessageType;
import com.yahoo.pasc.paxos.messages.PaxosMessage;
import com.yahoo.pasc.paxos.messages.Reply;
import com.yahoo.pasc.paxos.messages.Request;
import com.yahoo.pasc.paxos.state.ClientTimestamp;

public class ManualEncoder implements ChannelDownstreamHandler {

    @SuppressWarnings("unused")
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
//        LOG.trace("Encoding {} ", originalMessage);
//        MessageType type = MessageType.getMessageType(originalMessage);
//        messages[type.ordinal()]++;
//        nMessages++;
//        System.out.println("Encoding message " + originalMessage);
//        if (nMessages % 10000 == 0) {
//            MessageType values[] = MessageType.values();
//            StringBuilder sb = new StringBuilder();
//            for (int i = 0; i < values.length; ++i) {
//                sb.append(values[i].name()).append(':').append(messages[i]).append('\t');
//            }
//            System.out.println(sb.toString());
//        }
        int size = getSize(originalMessage, true);
        ChannelBuffer encodedMessage = encode(originalMessage, true, size, pool.getDirectBuffer(size));

        ChannelFuture future = e.getFuture();

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
        case INLINEREQ:{
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
                for (byte [] request : requests) {
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
        default:
            throw new UnsupportedOperationException("Unknown message type " + type);
        }

        return buffer;
    }
}
