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
import com.yahoo.pasc.paxos.messages.Accept;
import com.yahoo.pasc.paxos.messages.Accepted;
import com.yahoo.pasc.paxos.messages.Digest;
import com.yahoo.pasc.paxos.messages.Hello;
import com.yahoo.pasc.paxos.messages.InlineRequest;
import com.yahoo.pasc.paxos.messages.MessageType;
import com.yahoo.pasc.paxos.messages.PaxosMessage;
import com.yahoo.pasc.paxos.messages.Reply;
import com.yahoo.pasc.paxos.messages.Request;
import com.yahoo.pasc.paxos.state.ClientTimestamp;

public class ManualDecoder extends FrameDecoder {

    private static final Logger LOG = LoggerFactory.getLogger(ManualDecoder.class);
    protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buf) throws Exception {
        Object result = decode2(ctx, channel, buf);
        if (result != null) {
//            System.out.println("Decoded message " + result);
            PaxosMessage message = (PaxosMessage) result;
//            if (!message.verify()) {
//                throw new RuntimeException("Invalid message " + message);
//            }
            message.setCloned(PascRuntime.clone(message));
        }
        return result;
    }

    protected Object decode2(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buf) throws Exception {
        
        if (buf.readableBytes() < 4) return null;

        buf.markReaderIndex();
        
        int length = buf.readInt();
        length -= 4; // length has already been read
        
        if (buf.readableBytes() < length) {
            buf.resetReaderIndex();
            return null;
        }

//        try {
            long crc = buf.readLong();
            length -= 8; // crc has been read
            
            byte[] bytearray = new byte[length];
            buf.markReaderIndex();
            buf.readBytes(bytearray, 0, length);
            buf.resetReaderIndex();
            Checksum crc32 = CRC32Pool.getCRC32();
            crc32.reset();
            
            crc32.update(bytearray, 0, bytearray.length);
            
            long result = crc32.getValue();

//            LOG.trace("Decoding message with bytes {} computed CRC {} received CRC {}", new Object[] {bytearray, result, crc});
          
            if (result != crc) {
//                throw new RuntimeException("Invalid CRC");
                LOG.error("Invalid CRC");
            }
            
            CRC32Pool.pushCRC32(crc32);
            
            byte b = buf.readByte();
            int len;
            MessageType type = MessageType.values()[b];
            switch(type) {
            case REQUEST: 
            case INLINEREQ: {
//                System.out.println("Decoding submit");
                Request s = type.equals(MessageType.REQUEST) ? new Request() : new InlineRequest();
                s.setCRC(crc);
                s.setClientId(buf.readInt());
                s.setTimestamp(buf.readLong());
                len = buf.readInt();
                byte [] request = new byte [len];
                buf.readBytes(request);
                s.setRequest(request);
                return s;
            }
            case ACCEPT: {
//                System.out.println("Decoding accept");
                int senderId = buf.readInt();
                int ballot = buf.readInt();
                long iid = buf.readLong();
                len = buf.readInt();
                ClientTimestamp[] values = new ClientTimestamp[len];
//                System.out.println("Values: " + length);
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
                        if (reqSize == -1) continue;
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
//                System.out.println("Decoding accepted");
                Accepted ad = new Accepted();
                ad.setCRC(crc);
                ad.setSenderId(buf.readInt());
                ad.setBallot(buf.readInt());
                ad.setIid(buf.readLong());
                return ad;
            }
            case REPLY: {
//                System.out.println("Decoding reply");
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
            }
            buf.resetReaderIndex();
            throw new IllegalArgumentException("Unknown message type " + b + " " + type);
//        } catch (IndexOutOfBoundsException e) {
//            // Not enough byte in the buffer, reset to the start for the next
//            // try
////            System.out.println("Out of bounds");
//            buf.resetReaderIndex();
//            return null;
//        }
    }

}
