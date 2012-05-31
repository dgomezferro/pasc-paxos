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

import java.util.Arrays;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.pasc.paxos.messages.serialization.PureJavaCrc32;

public class SimpleClient implements ClientInterface {

    private static final Logger LOG = LoggerFactory.getLogger(SimpleClient.class);
    
    private PaxosInterface paxos;
    private byte[] payload;
    private Random random = new Random();
    private byte[] expected;
    
    public SimpleClient(int requestSize) {
        payload = new byte[requestSize];
    }

    @Override
    public void connected() {
//        expected = execute(payload);
        paxos.submitNewRequest(payload);
        
    }
    @Override
    public void messageReceived(byte[] message) {
//        if (!Arrays.equals(message, expected)) {
//            LOG.warn("State divergence. Wrong response. \n Stored: {} \n Received: {} \n From: {}", 
//                    expected, message);
//        }
        random.nextBytes(payload);
//        expected = execute(payload);
        paxos.submitNewRequest(payload);
    }
    
    public void setInterface(PaxosInterface paxosInterface) {
        paxos = paxosInterface;
    };

//    private PureJavaCrc32 crc32 = new PureJavaCrc32();
    long crc = 0;

//    private byte[] execute(byte[] command) {
//        crc32.reset();
//        crc32.update(command, 0, command.length);
//        byte[] t = toByta(crc);
//        crc32.update(t, 0, t.length);
//        crc = crc32.getValue();
//        return toByta(crc);
//    }

    public static byte[] toByta(long data) {
        return new byte[] { (byte) ((data >> 56) & 0xff), (byte) ((data >> 48) & 0xff), (byte) ((data >> 40) & 0xff),
                (byte) ((data >> 32) & 0xff), (byte) ((data >> 24) & 0xff), (byte) ((data >> 16) & 0xff),
                (byte) ((data >> 8) & 0xff), (byte) ((data >> 0) & 0xff), };
    }
}
