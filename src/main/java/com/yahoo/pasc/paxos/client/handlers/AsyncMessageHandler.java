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

package com.yahoo.pasc.paxos.client.handlers;

import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.pasc.Message;
import com.yahoo.pasc.MessageHandler;
import com.yahoo.pasc.paxos.client.ClientState;
import com.yahoo.pasc.paxos.client.TimestampMessage;
import com.yahoo.pasc.paxos.client.messages.Received;
import com.yahoo.pasc.paxos.messages.AsyncMessage;

public class AsyncMessageHandler implements MessageHandler<AsyncMessage, ClientState, Received.Descriptor> {
    
    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(AsyncMessageHandler.class);

    @Override
    public boolean guardPredicate(AsyncMessage receivedMessage) {
        return true;
    }

    @Override
    public List<Received.Descriptor> processMessage(AsyncMessage asyncMessage, ClientState state) {
        List<Received.Descriptor> descriptors = null;
        if (matches(asyncMessage, state)) {
            TimestampMessage tm = state.getAsyncMessage(asyncMessage.getTimestamp());
            if (tm == null || tm.getTimestamp() < asyncMessage.getTimestamp()) {
                tm = new TimestampMessage(asyncMessage.getTimestamp(), asyncMessage.getMessage());
                state.setAsyncMessage(asyncMessage.getTimestamp(), tm);
            }
            if (tm.isDelivered()) {
                return descriptors;
            }
            BitSet acks = tm.getAcks();
            acks.set(asyncMessage.getServerId());

            if (acks.cardinality() >= state.getQuorum()) {
                tm.setDelivered(true);
                descriptors = Arrays.asList(new Received.Descriptor(asyncMessage.getMessage()));
            }
        }
        return descriptors;
    }

    @Override
    public List<Message> getSendMessages(ClientState state, List<Received.Descriptor> descriptors) {
        if (descriptors != null && descriptors.size() > 0) {
            return Arrays.<Message> asList(new Received(descriptors.get(0).getValue()));
        }
        return null;
    }

    private boolean matches(AsyncMessage asyncMessage, ClientState state) {
        if (asyncMessage.getClientId() != state.getClientId())
            return false;
        return true;
    }
}
