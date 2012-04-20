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

package com.yahoo.paxos.handlers.learner;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.pasc.Message;
import com.yahoo.paxos.handlers.PaxosHandler;
import com.yahoo.paxos.handlers.proposer.ProposerRequest;
import com.yahoo.paxos.messages.Accepted;
import com.yahoo.paxos.messages.Execute;
import com.yahoo.paxos.messages.PaxosDescriptor;
import com.yahoo.paxos.state.IidAcceptorsCounts;
import com.yahoo.paxos.state.PaxosState;

public class Learner extends PaxosHandler<Accepted> {

    private static final Logger LOG = LoggerFactory.getLogger(Learner.class);

    @Override
    public boolean guardPredicate(Message receivedMessage) {
        return receivedMessage instanceof Accepted;
    }

    @Override
    public List<PaxosDescriptor> processMessage(Accepted message, PaxosState state) {
        long iid = message.getIid();
        long firstInstanceId = state.getFirstInstanceId();
        int maxInstances = state.getMaxInstances();
        if (iid < firstInstanceId || firstInstanceId + maxInstances <= iid) {
//            LOG.warn("Rejecting accepted. firstId: {} iid: {}", firstInstanceId, iid);
            return null;
        }
        // TODO check this
        long currentBallot = state.getBallotAcceptor();
        long ballot = message.getBallot();
        if (ballot < currentBallot) {
            LOG.warn("Rejecting accepted. msg ballot: {} current ballot: {}", ballot,
                    currentBallot);
            return null;
        }
        List<PaxosDescriptor> descriptors = new ArrayList<PaxosDescriptor>();
        
        IidAcceptorsCounts instance = state.getAcceptedElement(iid);
        if (instance == null || instance.getIid() != iid) {
            instance = new IidAcceptorsCounts(iid);
            state.setAcceptedElement(iid, instance);
        }
        instance.setAcceptor(message.getSenderId());

        checkExecute(iid, state, descriptors, null);
        
        return descriptors;
    }

    public static void checkExecute(long checkIid, PaxosState state, List<PaxosDescriptor> descriptors) {
        checkExecute(checkIid, state, descriptors, null);
    }

    public static void checkExecute(long checkIid, PaxosState state, List<PaxosDescriptor> descriptors, Accepted msg) {
//        LOG.trace("Checking if instance {} is learned with message {}.", checkIid, msg);
        long maxExecuted = state.getMaxExecuted();
        long firstInstanceId = state.getFirstInstanceId();
        int maxInstances = state.getMaxInstances();
        int quorum = state.getQuorum();
        int servers = state.getServers();
        boolean isLeader = state.getIsLeader();
        
        for (long iid = checkIid; iid < firstInstanceId + maxInstances; ++iid) {
//            LOG.trace("Checking instance {}.", iid);
            if (iid != maxExecuted + 1) {
//                    LOG.trace("Next instance to be executed is {} and we are at {}", maxExecuted + 1, iid);
                return;
            }
            
            IidAcceptorsCounts instance = state.getAcceptedElement(iid);
            if (instance == null || instance.getIid() != iid) {
                return;
            }
            if (instance.isAccepted() && instance.getCardinality(servers) >= quorum) {
//                LOG.trace("This instance has been accepted.");


                if (isLeader) {
//                    LOG.trace("Decrement pending.");
                    ProposerRequest.decrementPending(state, descriptors);
                }

                // execute instance
//                LOG.trace("Execute instance " + iid);
                maxExecuted++;
                state.setMaxExecuted(maxExecuted);
                descriptors.add(new Execute.Descriptor(iid));
            } else {
//                if (instance.isAccepted())
//                    LOG.trace("This instance doesn't have a quorum of accepts yet.");
//                else
//                    LOG.trace("This instance hasn't been accepted yet.");
            }
        }
    }
}
