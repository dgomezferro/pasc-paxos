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

package com.yahoo.paxos.messages.serialization;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferFactory;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.buffer.DirectChannelBufferFactory;

public class BufferPool {

//    private Deque<ChannelBuffer> pool = new ArrayDeque<ChannelBuffer>();
//    private Deque<ChannelBuffer> directPool = new ArrayDeque<ChannelBuffer>();
    private ChannelBufferFactory factory = new DirectChannelBufferFactory(256*1024);
    
    public synchronized ChannelBuffer getBuffer(int size) {
        return ChannelBuffers.buffer(size);
    }

    public synchronized ChannelBuffer getDirectBuffer(int size) {
        return factory.getBuffer(size);
//        if (directPool.isEmpty()) {
////            System.out.println("Allocated new Direct Buffer for thread " + Thread.currentThread().getId());
//            return ChannelBuffers.directBuffer(50);
//        }
//        return directPool.pollLast();
    }
    

//    public synchronized void pushBuffer(ChannelBuffer buffer) {
//        if (buffer.isDirect()) {
//            directPool.add(buffer);
//        } else {
//            pool.add(buffer);
//        }
//    }
}
