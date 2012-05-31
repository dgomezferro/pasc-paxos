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

package com.yahoo.pasc.paxos.handlers;

import java.util.ArrayList;
import java.util.List;

import com.yahoo.pasc.Message;
import com.yahoo.pasc.MessageHandler;
import com.yahoo.pasc.paxos.messages.PaxosDescriptor;
import com.yahoo.pasc.paxos.messages.PaxosMessage;
import com.yahoo.pasc.paxos.state.PaxosState;

public abstract class PaxosHandler<M extends PaxosMessage> 
    implements MessageHandler<M, PaxosState, PaxosDescriptor> {

    @Override
    public boolean guardPredicate(M receivedMessage) {
        return true;
    };

    @Override
    public List<Message> getOutputMessages(PaxosState state, List<PaxosDescriptor> descriptors) {
        if (descriptors == null) 
            return null;

        List<Message> messages = null;
        for (PaxosDescriptor desc : descriptors) {
            if (desc != null) {
                List<PaxosMessage> generated = desc.buildMessages(state);
                if (generated != null && !generated.isEmpty()) {
                    if (messages == null)
                        messages = new ArrayList<Message>(generated);
                    else
                        messages.addAll(generated);
                }
            }
        }
        return messages;
    }
}
