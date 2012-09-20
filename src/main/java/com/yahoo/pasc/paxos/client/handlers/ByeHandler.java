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
import com.yahoo.pasc.paxos.client.Reconnect;
import com.yahoo.pasc.paxos.messages.Bye;
import com.yahoo.pasc.paxos.messages.Hello;

public class ByeHandler implements MessageHandler<Bye, ClientState, Reconnect> {

    @Override
    public boolean guardPredicate(Bye receivedMessage) {
        return true;
    }

    @Override
    public List<Reconnect> processMessage(Bye bye, ClientState state) {
        List<Reconnect> descriptors = null;
        if (!matches(bye, state)) {
            return null;
        }
        int disconnected = state.getDisconnected();
        disconnected++;
        state.setDisconnected(disconnected);
        int maxFaults = state.getServers() / 2 - 1;
        if (disconnected == maxFaults) {
            // Send the first message if connected to all servers
            descriptors = Arrays.asList(new Reconnect());
        }
        return descriptors;
    }

    private boolean matches(Bye bye, ClientState state) {
        Hello hello = state.getPendingHello();
        if (hello == null)
            return false;
        return hello.getClientId() == bye.getClientId();
    }

    @Override
    public List<Message> getOutputMessages(ClientState state, List<Reconnect> descriptors) {
        if (descriptors != null && descriptors.size() > 0) {
            return Arrays.<Message> asList(new Reconnect());
        }
        return null;
    }
}
