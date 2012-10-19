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

package com.yahoo.pasc.paxos.handlers.acceptor;

import it.unimi.dsi.fastutil.longs.LongArrayList;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.pasc.Message;
import com.yahoo.pasc.paxos.handlers.PaxosHandler;
import com.yahoo.pasc.paxos.handlers.learner.Learner;
import com.yahoo.pasc.paxos.messages.PaxosDescriptor;
import com.yahoo.pasc.paxos.messages.Prepare;
import com.yahoo.pasc.paxos.messages.Prepared;
import com.yahoo.pasc.paxos.state.InstanceRecord;
import com.yahoo.pasc.paxos.state.PaxosState;
import com.yahoo.pasc.paxos.state.IidAcceptorsCounts;

public class AcceptorPrepare extends PaxosHandler<Prepare> {

    private static final Logger LOG = LoggerFactory.getLogger(AcceptorPrepare.class);

    @Override
    public List<PaxosDescriptor> processMessage(Prepare message, PaxosState state) {

        // check that the leader is newer than the last one by looking at the ballot number
        int currentBallot = state.getBallotAcceptor();
        if (message.getBallot() <= currentBallot) {
            LOG.trace("Rejecting propose. msg ballot: {} current ballot: {}", message.getBallot(), currentBallot);
            return null;
        }
        state.setBallotAcceptor(message.getBallot());

        long checkpointPeriod = state.getCheckpointPeriod();
        long minIid = message.getMaxExecutedIid();
        long maxExecuted = state.getMaxExecuted();
        long maxForgotten = (state.getFirstDigestId() - 1) * checkpointPeriod;

        if (LOG.isTraceEnabled()) {
            LOG.trace("Processing propose. minIid {} maxExecuted {} maxForgotten {}", 
                    new Object[] { minIid, maxExecuted, maxForgotten });
        }

        List<PaxosDescriptor> descriptors = new ArrayList<PaxosDescriptor>(1);

        // send learned (executed) requests
        LongArrayList learnedReqs = new LongArrayList();
        long currIid = Math.max(minIid + 1, maxForgotten + 1);
        while (currIid < minIid + state.getMaxInstances()) {
            InstanceRecord instance = state.getInstancesElement(currIid);
            IidAcceptorsCounts acceptors = state.getAcceptedElement(currIid);
            // checks that the iid is correct and that the instance is "fully" accepted, i.e., it has all requests
            if (instance != null && instance.getIid() == currIid && acceptors != null && acceptors.isAccepted()) {
                LOG.trace("Adding learned req with iid {}", currIid);
                learnedReqs.add(currIid);
            }
            currIid++;
        }

        // send accepted requests
        currIid = Math.max(minIid + 1, maxForgotten + 1);
        LongArrayList acceptedReqs = new LongArrayList();
        while (currIid < minIid + state.getMaxInstances()) {
            InstanceRecord instance = state.getInstancesElement(currIid);
            if (instance != null && instance.getIid() == currIid) {
                LOG.trace("Adding accepted instance with iid {}", currIid);
                acceptedReqs.add(currIid);
            } else {
                LOG.trace("Not adding accepted instance for iid {} instance {}", currIid, instance);
            }
            currIid++;
        }

        // send the current checkpoint digest - the state machine will fetch the checkpoint independently
        if (minIid < maxForgotten) {
            descriptors.add(new Prepared.Descriptor(message.getBallot(), message.getSenderId(), acceptedReqs,
                    learnedReqs, maxForgotten, state.getQuorum()));
        } else {
            descriptors.add(new Prepared.Descriptor(message.getBallot(), message.getSenderId(), acceptedReqs,
                    learnedReqs));
        }

        return descriptors;
    }

}
