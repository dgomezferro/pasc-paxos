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

import java.util.zip.Checksum;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.pasc.PascRuntime;
import com.yahoo.pasc.exceptions.InputMessageException;
import com.yahoo.pasc.paxos.messages.Accept;
import com.yahoo.pasc.paxos.messages.Accepted;
import com.yahoo.pasc.paxos.messages.AsyncMessage;
import com.yahoo.pasc.paxos.messages.Bye;
import com.yahoo.pasc.paxos.messages.ControlMessage;
import com.yahoo.pasc.paxos.messages.Digest;
import com.yahoo.pasc.paxos.messages.Hello;
import com.yahoo.pasc.paxos.messages.InlineRequest;
import com.yahoo.pasc.paxos.messages.MessageType;
import com.yahoo.pasc.paxos.messages.PaxosMessage;
import com.yahoo.pasc.paxos.messages.Prepare;
import com.yahoo.pasc.paxos.messages.Prepared;
import com.yahoo.pasc.paxos.messages.Reply;
import com.yahoo.pasc.paxos.messages.Request;
import com.yahoo.pasc.paxos.messages.ServerHello;
import com.yahoo.pasc.paxos.state.ClientTimestamp;
import com.yahoo.pasc.paxos.state.DigestidDigest;
import com.yahoo.pasc.paxos.state.InstanceRecord;

public class ManualDecoder extends FrameDecoder {

    private static final Logger LOG = LoggerFactory.getLogger(ManualDecoder.class);

    protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buf) throws Exception {
        Object result = decode2(ctx, channel, buf);
        if (result != null) {
            PaxosMessage message = (PaxosMessage) result;
            message.setCloned(PascRuntime.clone(message));
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("Received msg: {}", result);
        }
        return result;
    }

    protected Object decode2(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buf) throws Exception {

        if (buf.readableBytes() < 4)
            return null;

        buf.markReaderIndex();

        int length = buf.readInt();
        length -= 4; // length has already been read
        
        if (buf.readableBytes() < length) {
            buf.resetReaderIndex();
            return null;
        }

        long crc = buf.readLong();
        length -= 8; // crc has been read

        if (length == 0) {
            LOG.warn("Length is 0.");
        }

        byte[] bytearray = new byte[length];
        buf.markReaderIndex();
        buf.readBytes(bytearray, 0, length);
        buf.resetReaderIndex();
        Checksum crc32 = CRC32Pool.getCRC32();
        crc32.reset();

        crc32.update(bytearray, 0, bytearray.length);

        long result = crc32.getValue();

        if (LOG.isTraceEnabled()) {
            LOG.trace("Decoding message with bytes {} computed CRC {} received CRC {}", new Object[] { bytearray,
                    result, crc });
        }

        if (result != crc) {
            byte b = buf.readByte();
            MessageType type = MessageType.values()[b];
            LOG.error("Invalid CRC for {}. Expected {} Actual {}", new Object[] {type, crc, result});
            throw new InputMessageException("Invalid CRC", null, null);
        }

        CRC32Pool.pushCRC32(crc32);

        byte b = buf.readByte();
        int len;
        MessageType type = MessageType.values()[b];
        switch (type) {
        case REQUEST:
        case INLINEREQ: {
            Request s = type.equals(MessageType.REQUEST) ? new Request() : new InlineRequest();
            s.setCRC(crc);
            s.setClientId(buf.readInt());
            s.setTimestamp(buf.readLong());
            len = buf.readInt();
            byte[] request = new byte[len];
            buf.readBytes(request);
            s.setRequest(request);
            return s;
        }
        case ACCEPT: {
            int senderId = buf.readInt();
            int ballot = buf.readInt();
            long iid = buf.readLong();
            len = buf.readInt();
            ClientTimestamp[] values = new ClientTimestamp[len];
            for (int i = 0; i < len; ++i) {
                ClientTimestamp value = new ClientTimestamp();
                value.setClientId(buf.readInt());
                value.setTimestamp(buf.readLong());
                values[i] = value;
            }
            byte[][] requests = null;
            int requestsSize = buf.readInt();
            if (requestsSize != -1) {
                requests = new byte[requestsSize][];
                for (int i = 0; i < requestsSize; ++i) {
                    int reqSize = buf.readInt();
                    if (reqSize == -1)
                        continue;
                    byte[] request = new byte[reqSize];
                    buf.readBytes(request);
                    requests[i] = request;
                }
            }
            Accept a = new Accept(senderId, iid, ballot, values, len);
            a.setRequests(requests);
            a.setCRC(crc);
            return a;
        }
        case ACCEPTED: {
            int senderId = buf.readInt();
            int ballot = buf.readInt();
            long iid = buf.readLong();
            Accepted ad = new Accepted(senderId, ballot, iid);
            ad.setCRC(crc);
            return ad;
        }
        case REPLY: {
            Reply r = new Reply();
            r.setCRC(crc);
            r.setServerId(buf.readInt());
            r.setClientId(buf.readInt());
            r.setTimestamp(buf.readLong());
            len = buf.readInt();
            byte[] v = new byte[len];
            buf.readBytes(v);
            r.setValue(v);
            return r;
        }
        case DIGEST: {
            Digest d = new Digest();
            d.setCRC(crc);
            d.setSenderId(buf.readInt());
            d.setDigestId(buf.readLong());
            d.setDigest(buf.readLong());
            return d;
        }
        case HELLO: {
            Hello h = new Hello(buf.readInt());
            h.setCRC(crc);
            return h;
        }
        case SERVERHELLO: {
            ServerHello h = new ServerHello(buf.readInt(), buf.readInt());
            h.setCRC(crc);
            return h;
        }
        case BYE: {
            Bye h = new Bye(buf.readInt(), buf.readInt());
            h.setCRC(crc);
            return h;
        }
        case PREPARE: {
            int senderId = buf.readInt();
            int ballot = buf.readInt();
            long maxExecutedIid = buf.readLong();
            Prepare p = new Prepare(senderId, ballot, maxExecutedIid);
            p.setCRC(crc);
            return p;
        }
        case PREPARED: {
            int senderId = buf.readInt();
            int receiver = buf.readInt();
            int replyBallot = buf.readInt();

            int acceptedSize = buf.readInt();
            InstanceRecord[] acceptedReqs = new InstanceRecord[acceptedSize];
            for (int i = 0; i < acceptedSize; i++) {
                long iid = buf.readLong();
                int ballot = buf.readInt();
                int arraySize = buf.readInt();
                ClientTimestamp[] ct = new ClientTimestamp[arraySize];
                for (int j = 0; j < arraySize; j++) {
                    int clientId = buf.readInt();
                    long timestamp = buf.readLong();
                    ct[j] = new ClientTimestamp(clientId, timestamp);
                }
                acceptedReqs[i] = new InstanceRecord(iid, ballot, ct, arraySize);
            }

            int learnedSize = buf.readInt();
            Accept[] learnedReqs = new Accept[learnedSize];
            for (int i = 0; i < learnedSize; i++) {
                learnedReqs[i] = (Accept) decode2(ctx, channel, buf);
            }

            int digestId = buf.readInt();
            long digest = buf.readLong();
            DigestidDigest checkpointDigest = new DigestidDigest(digestId, digest);

            long maxForgotten = buf.readLong();

            Prepared pd = new Prepared(senderId, receiver, replyBallot, acceptedSize, acceptedReqs, learnedSize,
                    learnedReqs, checkpointDigest, maxForgotten);
            pd.setCRC(crc);
            return pd;
        }
        case ASYNC_MESSAGE: {
            int clientId = buf.readInt();
            int serverId = buf.readInt();
            long ts = buf.readLong();
            len = buf.readInt();
            byte [] message = new byte [len];
            buf.readBytes(message);
            AsyncMessage am = new AsyncMessage(clientId, serverId, ts, message);
            am.setCRC(crc);
            return am;
        }
        case CONTROL: {
            int clientId = buf.readInt();
            len = buf.readInt();
            byte [] message = new byte [len];
            buf.readBytes(message);
            ControlMessage cm = new ControlMessage(clientId, message);
            cm.setCRC(crc);
            return cm;
        }
        }
        buf.resetReaderIndex();
        throw new IllegalArgumentException("Unknown message type " + b + " " + type);
    }

}
