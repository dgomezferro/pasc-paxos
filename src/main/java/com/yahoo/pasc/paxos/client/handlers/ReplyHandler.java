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
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.pasc.Message;
import com.yahoo.pasc.MessageHandler;
import com.yahoo.pasc.paxos.client.ClientState;
import com.yahoo.pasc.paxos.client.ReplyStore;
import com.yahoo.pasc.paxos.client.messages.Received;
import com.yahoo.pasc.paxos.messages.Reply;

public class ReplyHandler implements MessageHandler<Reply, ClientState, Received.Descriptor> {
    
    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(ReplyHandler.class);

    @Override
    public boolean guardPredicate(Reply receivedMessage) {
        return true;
    }

    @Override
    public List<Received.Descriptor> processMessage(Reply reply, ClientState state) {
        List<Received.Descriptor> descriptors = null;
        if (matches(reply, state)) {
            ReplyStore store = state.getReplyStore();
            int quorum = state.getQuorum();
            if (store.getMatch(quorum) != -1) {
                // Already delivered message
                return null;
            }
            store.addRemote(reply.getServerId(), reply);
            byte[] value = store.getStableReply(quorum);
            if (value != null) {
                descriptors = Arrays.asList(new Received.Descriptor(value));
                state.setReplyStore(new ReplyStore(state.getServers()));
            }
        }
        return descriptors;
    }

    @Override
    public List<Message> getOutputMessages(ClientState state, List<Received.Descriptor> descriptors) {
        if (descriptors != null && descriptors.size() > 0) {
            return Arrays.<Message> asList(new Received(descriptors.get(0).getValue()));
        }
        return null;
    }

    private boolean matches(Reply reply, ClientState state) {
        if (reply.getClientId() != state.getClientId())
            return false;
        if (reply.getTimestamp() != state.getPendingRequest().getTimestamp())
            return false;
        return true;
    }
}
