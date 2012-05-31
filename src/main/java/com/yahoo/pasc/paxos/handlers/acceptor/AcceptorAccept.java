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

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.pasc.Message;
import com.yahoo.pasc.paxos.handlers.PaxosHandler;
import com.yahoo.pasc.paxos.handlers.proposer.ProposerRequest;
import com.yahoo.pasc.paxos.messages.Accept;
import com.yahoo.pasc.paxos.messages.Accepted;
import com.yahoo.pasc.paxos.messages.PaxosDescriptor;
import com.yahoo.pasc.paxos.state.ClientTimestamp;
import com.yahoo.pasc.paxos.state.IidAcceptorsCounts;
import com.yahoo.pasc.paxos.state.IidRequest;
import com.yahoo.pasc.paxos.state.PaxosState;

public class AcceptorAccept extends PaxosHandler<Accept> {

    private static final Logger LOG = LoggerFactory.getLogger(ProposerRequest.class);

    @Override
    public List<PaxosDescriptor> processMessage(Accept message, PaxosState state) {
        if (state.getIsLeader())
            return null;

        int ballot = message.getBallot();
        int currentBallot = state.getBallotAcceptor();
        if (ballot < currentBallot) {
            LOG.trace("Rejecting accept. msg ballot: {} current ballot: {}", ballot, currentBallot);
            // We promised not to accept ballots lower than our current ballot
            return null;
        }
        long iid = message.getIid();
        long firstInstanceId = state.getFirstInstanceId();

        if (firstInstanceId <= iid && iid < firstInstanceId + state.getMaxInstances()) {

            ClientTimestamp[] cts = message.getValues();
            byte[][] requests = message.getRequests();
            int arraySize = message.getArraySize();
            int countReceivedRequests = 0;

            for (int i = 0; i < arraySize; ++i) {
                if (requests != null && requests[i] != null) {
                    state.setReceivedRequest(cts[i], new IidRequest(iid, requests[i]));
                    countReceivedRequests++;
                } else {
                    IidRequest request = state.getReceivedRequest(cts[i]);
                    if (request == null || (request.getIid() != -1 && request.getIid() < firstInstanceId)) {
                        request = new IidRequest(iid);
                        state.setReceivedRequest(cts[i], request);
                    } else if (request.getRequest() != null && request.getIid() == -1) {
                        request.setIid(iid);
                        countReceivedRequests++;
                    } else {
                        LOG.warn("The acceptor created this request. Duplicated accept?");
                    }
                }
            }
            IidAcceptorsCounts accepted = state.getAcceptedElement(iid);
            if (accepted == null || accepted.getIid() != iid || accepted.getBallot() < currentBallot) {
                accepted = new IidAcceptorsCounts(iid, message.getBallot());
                state.setAcceptedElement(iid, accepted);
            }
            accepted.setReceivedRequests(countReceivedRequests);
            accepted.setTotalRequests(arraySize);
            state.setInstancesElement(iid, message.getInstance());

            List<PaxosDescriptor> descriptors = new ArrayList<PaxosDescriptor>();

            checkAccept(iid, accepted, state, descriptors);

            return descriptors;
        } else if (LOG.isTraceEnabled()) {
            LOG.trace("Received invalid accept: {} iid: {} firstIid: {} maxInst: {}", new Object[] { message, iid,
                    firstInstanceId, state.getMaxInstances() });
        }
        return null;
    }

    public static void checkAccept(long iid, PaxosState state, List<PaxosDescriptor> descriptors) {
        checkAccept(iid, null, state, descriptors);
    }

    public static void checkAccept(long iid, IidAcceptorsCounts accepted, PaxosState state,
            List<PaxosDescriptor> descriptors) {
        if (accepted == null) {
            // Called from proposer, a new request has been received
            accepted = state.getAcceptedElement(iid);
            accepted.setReceivedRequests(accepted.getReceivedRequests() + 1);
        }

        int receivedRequests = accepted.getReceivedRequests();
        int totalRequests = accepted.getTotalRequests();
        if (receivedRequests == totalRequests) {
            accepted.setAccepted(true);

            descriptors.add(new Accepted.Descriptor(iid));
        }
    }
}
