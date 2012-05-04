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

package com.yahoo.pasc.paxos.messages;

import java.io.Serializable;
import java.util.zip.Checksum;

import org.jboss.netty.buffer.ChannelBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.pasc.Message;
import com.yahoo.pasc.paxos.messages.serialization.BufferPool;
import com.yahoo.pasc.paxos.messages.serialization.CRC32Pool;
import com.yahoo.pasc.paxos.messages.serialization.ManualEncoder;

public abstract class PaxosMessage extends Message implements Serializable {
    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(PaxosMessage.class);

    private static final long serialVersionUID = 8622528928855536990L;
    private static ManualEncoder encoder = new ManualEncoder();

//    private ThreadLocal<BufferPool> pool = new ThreadLocal<BufferPool>() {
//        protected BufferPool initialValue() {
//            return new BufferPool();
//        };
//    };
    
    private transient long crc;

//    @Optional("bytecopy")
//    transient private ChannelBuffer bytecopy = null;
    
    private ChannelBuffer getSerializedMessage(Message m) {
//        long threadId = Thread.currentThread().getId();
        BufferPool pool = encoder.getPool();
        
        int size = ManualEncoder.getSize(this, false);
        return encoder.encode((PaxosMessage) m, false, size, pool.getBuffer(size));
        
//        int position = serializeBuffer.readableBytes();
//        byte[] copy = new byte[position];
//        serializeBuffer.readBytes(copy);
        
//        pool.pushBuffer(serializeBuffer);
        
//        return copy;
    }

    public long computeCRC(Message m, byte [] bytearray) {
        Checksum crc32 = CRC32Pool.getCRC32();
        crc32.reset();
        
        
        crc32.update(bytearray, 0, bytearray.length);
        
        long result = crc32.getValue();
        
//        LOG.trace("Computing CRC for message {} with message {} bytearay {} crc {}", new Object[]{this, m, bytearray, result});

        CRC32Pool.pushCRC32(crc32);
        
        return result;
    }
    private long computeCRC(Message m) {
        ChannelBuffer bytecopy = getSerializedMessage(m);
        byte [] bytearray = bytecopy.array();
        return computeCRC(m, bytearray);
    }

    @Override
    public void storeReplica(Message m) {
        this.crc = computeCRC(m);
    }
    
    public void setCRC(long crc) {
        this.crc = crc;
    }
    
    public long getCRC() {
        return crc;
    }
    
	// true if message is correct
    @Override
	protected boolean verify() {
        return this.crc == computeCRC(this);
	}
    
    @Override
    public String toString() {
        return "< PM with crc field:" + crc + " >"; 
    }
}
