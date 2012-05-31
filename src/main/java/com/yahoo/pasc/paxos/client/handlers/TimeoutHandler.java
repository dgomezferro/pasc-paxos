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

import com.yahoo.pasc.Message;
import com.yahoo.pasc.MessageHandler;
import com.yahoo.pasc.paxos.client.ClientState;
import com.yahoo.pasc.paxos.client.messages.Timeout;
import com.yahoo.pasc.paxos.messages.Request;

public class TimeoutHandler implements MessageHandler<Timeout, ClientState, Request> {

    @Override
    public boolean guardPredicate(Timeout receivedMessage) {
        return true;
    }

    @Override
    public List<Request> processMessage(Timeout timeout, ClientState state) {
        if (timeout.getTimestamp() == state.getTimestamp()) {
            Request pendingRequest = state.getPendingRequest();
            return Arrays.asList(new Request(pendingRequest.getClientId(), pendingRequest.getTimestamp(), pendingRequest.getRequest()));
        }
        return null;
    }

    @Override
    public List<Message> getSendMessages(ClientState state, List<Request> messages) {
        if (messages != null && messages.size() > 0) {
            return Arrays.<Message>asList(messages.get(0));
        }
        return null;
    }
}
