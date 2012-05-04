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

package com.yahoo.pasc.paxos.handlers.learner;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.pasc.Message;
import com.yahoo.pasc.paxos.handlers.DigestHandler;
import com.yahoo.pasc.paxos.handlers.PaxosHandler;
import com.yahoo.pasc.paxos.messages.Digest;
import com.yahoo.pasc.paxos.messages.PaxosDescriptor;
import com.yahoo.pasc.paxos.messages.PreReply;
import com.yahoo.pasc.paxos.messages.Reply;
import com.yahoo.pasc.paxos.state.PaxosState;
import com.yahoo.pasc.paxos.state.TimestampReply;

public class LearnerPreReply extends PaxosHandler<PreReply> {

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(LearnerPreReply.class);

    @Override
    public boolean guardPredicate(Message receivedMessage) {
        return receivedMessage instanceof PreReply;
    }

    @Override
    public List<PaxosDescriptor> processMessage(PreReply message, PaxosState state) {
        int clientId = message.getClientId();
        long timestamp = message.getTimestamp();
//        LOG.trace("Received {}", message);
        state.setReplyCacheElement(clientId, new TimestampReply(timestamp, message.getReply()));
        List<PaxosDescriptor> descriptors = null;
        Long digest = message.getDigest();
        if (digest != null) {
            long iid = message.getIid();
//            LOG.trace("Processing digest for ct {} and iid {}", ct, iid);
            long checkpointPeriod = state.getCheckpointPeriod();
            // only send digests to other servers if we have to coordinate them
            if (state.getDigestQuorum() > 1) {
                if (descriptors == null) 
                    descriptors = new ArrayList<PaxosDescriptor>(2);
                descriptors.add(new Digest.Descriptor(iid / checkpointPeriod, digest));
            }

            DigestHandler.storeDigest(iid / checkpointPeriod, digest, true, state);
        }
        if (!state.getIsLeader()) {
//            LOG.trace("Adding reply");
            if (descriptors == null)
                descriptors = new ArrayList<PaxosDescriptor>(1);
            descriptors.add(new Reply.Descriptor(clientId));
        }
        return descriptors;
    }
}
