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
import com.yahoo.pasc.paxos.client.Connected;
import com.yahoo.pasc.paxos.messages.Hello;

public class HelloHandler implements MessageHandler<Hello, ClientState, Connected> {

    @Override
    public boolean guardPredicate(Hello receivedMessage) {
        return true;
    }

    @Override
    public List<Connected> processMessage(Hello hello, ClientState state) {
        List<Connected> descriptors = null;
        int connected = state.getConnected();
        connected++;
        state.setConnected(connected);
        if (connected == state.getServers()) {
            // Send the first message if connected to all servers
            descriptors = Arrays.asList(new Connected());
        }
        return descriptors;
    }

    @Override
    public List<Message> getOutputMessages(ClientState state, List<Connected> descriptors) {
        if (descriptors != null && descriptors.size() > 0) {
            return Arrays.<Message> asList(new Connected());
        }
        return null;
    }
}
