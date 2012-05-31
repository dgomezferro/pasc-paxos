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

package com.yahoo.pasc.paxos.handlers.proposer;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.pasc.Message;
import com.yahoo.pasc.paxos.handlers.PaxosHandler;
import com.yahoo.pasc.paxos.messages.Accept;
import com.yahoo.pasc.paxos.messages.Accepted;
import com.yahoo.pasc.paxos.messages.DigestToSM;
import com.yahoo.pasc.paxos.messages.PaxosDescriptor;
import com.yahoo.pasc.paxos.messages.Prepared;
import com.yahoo.pasc.paxos.state.ClientTimestamp;
import com.yahoo.pasc.paxos.state.DigestStore;
import com.yahoo.pasc.paxos.state.DigestidDigest;
import com.yahoo.pasc.paxos.state.IidAcceptorsCounts;
import com.yahoo.pasc.paxos.state.IidRequest;
import com.yahoo.pasc.paxos.state.InstanceRecord;
import com.yahoo.pasc.paxos.state.PaxosState;

public class ProposerPrepared extends PaxosHandler<Prepared> {

    private static final Logger LOG = LoggerFactory.getLogger(ProposerPrepared.class);

    @Override
    public List<PaxosDescriptor> processMessage(Prepared receivedMessage, PaxosState state) {

        if (!state.getIsLeader() || state.getBallotProposer() != receivedMessage.getReplyBallot()) {
            return null;
        }

        // add message to array
        int senderId = receivedMessage.getSenderId();
        state.getPrepared().setPreparedMessage(receivedMessage, senderId);

        // used to get new checkpoint
        long checkpointPeriod = state.getCheckpointPeriod();
        long maxForgotten = state.getFirstDigestId() * checkpointPeriod;
        boolean newCheckpoint = false;
        DigestidDigest newDigest = null;

        List<PaxosDescriptor> descriptors = new ArrayList<PaxosDescriptor>();

        // verify quorum - local communication is shortcut
        int servers = state.getServers();
        int serverId = state.getServerId();
        long maxExecuted = state.getMaxExecuted();
        int quorum = state.getPrepared().getCardinality(servers);
        int neededQuorum = state.getQuorum();
        int ballotProposer = state.getBallotProposer();
        LOG.trace("quor {} needed {}", quorum, neededQuorum);
        if (quorum >= neededQuorum) {

            state.setCompletedPhaseOne(true);

            // execute recovery
            Prepared[] messages = state.getPrepared().getPreparedMessages();

            // recover initial checkpoint, if needed
            for (int i = 0; i < servers; i++) {
                if (i == serverId) {
                    continue;
                }
                LOG.trace("Recovering from server {}", i);
                // remember that the array element for the latest message is only set after completing the handler
                if (i == receivedMessage.getSenderId()) {
                    LOG.trace("Recovering from the message");
                    if (receivedMessage.getMaxForgotten() > maxForgotten) {
                        LOG.trace("Recovering from the message");
                        maxForgotten = receivedMessage.getMaxForgotten();
                        newDigest = receivedMessage.getCheckpointDigest().cloneDeep();
                        newCheckpoint = true;
                    } else {
                        LOG.trace("Nothing to recover");
                    }
                } else if (messages[i] != null && messages[i].getMaxForgotten() > maxForgotten) {
                    maxForgotten = messages[i].getMaxForgotten();
                    newDigest = messages[i].getCheckpointDigest().cloneDeep();
                    newCheckpoint = true;
                } else {

                }
            }

            if (newCheckpoint) {
                LOG.warn("New checkpoint. Installing digest {}", newDigest);

                // update execution pointer
                maxExecuted = maxForgotten;
                LOG.warn("Setting max executed to {} from {}.", maxExecuted, state.getMaxExecuted());
                state.setMaxExecuted(maxExecuted);
                state.setFirstInstanceId(maxExecuted);

                // set the new checkpoint as first checkpoint
                DigestStore newStore = new DigestStore(maxExecuted / checkpointPeriod, servers);
                newStore.setRecovered(newDigest.getDigest());
                state.setDigestStoreElement(newDigest.getDigestId(), newStore);
                state.setFirstDigestId(newStore.getDigestId());

                // send new checkpoint to server
                descriptors.add(new DigestToSM.Descriptor(newDigest.getDigest()));

            }
            LOG.trace("Setting currIid to {} from {}.", maxExecuted + 1, state.getCurrIid());
            state.setCurrIid(maxExecuted + 1);

            // recover subsequent elements
            int[] currLearnedPos = new int[servers];
            int[] currAcceptedPos = new int[servers];

            int pendingRequests = state.getPendingInstances();
            int bufSize = state.getBufferSize();

            for (long currIid = maxExecuted + 1; currIid < state.getMaxExecuted() + state.getMaxInstances(); currIid++) {

                int currBallot = -1;
                InstanceRecord currentInstance = state.getInstancesElement(currIid);
                if (currentInstance != null && currentInstance.getIid() == currIid) {
                    currBallot = state.getInstancesBallot(currIid);
                }

                InstanceRecord newRecord = null;
                byte[][] requests = null;

                for (int i = 0; i < servers; i++) {
                    if (i == serverId) {
                        continue;
                    }
                    Prepared currentMessage;
                    if (i == receivedMessage.getSenderId()) {
                        currentMessage = receivedMessage.cloneDeep();
                    } else if (messages[i] == null) {
                        continue;
                    } else {
                        currentMessage = messages[i].cloneDeep();
                    }
                    if (currLearnedPos[i] >= currentMessage.getLearnedReqs().length) {
                        continue;
                    }
                    Accept otherLearnedRecord = currentMessage.getLearnedReq(currLearnedPos[i]);
                    if (otherLearnedRecord.getIid() == currIid) {
                        currLearnedPos[i]++;
                        newRecord = otherLearnedRecord.getInstance();
                        requests = otherLearnedRecord.getRequests();
                        break;
                    }

                    InstanceRecord otherAcceptedRecord = currentMessage.getAcceptedReq(currAcceptedPos[i]);
                    if (otherAcceptedRecord.getIid() == currIid && otherAcceptedRecord.getBallot() > currBallot) {
                        newRecord = otherAcceptedRecord;
                        currBallot = otherAcceptedRecord.getBallot();
                    }
                }

                // apply currRecord to log and send the corresponding Accept message
                if (newRecord != null) {
                    LOG.warn("Applying record {} to iid {}.", newRecord, currIid);
                    newRecord.setBallot(ballotProposer);
                    state.setInstancesElement(currIid, newRecord);
                    if (requests != null) {
                        for (int i = 0; i < newRecord.getArraySize(); ++i) {
                            ClientTimestamp ct = newRecord.getClientTimestamp(i);
                            state.setReceivedRequest(ct, new IidRequest(currIid, requests[i]));
                        }
                    }
                    long lastCurrIid = state.getCurrIid();
                    // Add NOPs from latest currIid sent to currIid
                    for (long iid = lastCurrIid; iid < currIid; ++iid) {
                        LOG.warn("Setting NOP at {}.", iid);
                        InstanceRecord nopRecord = new InstanceRecord(iid, ballotProposer, new ClientTimestamp[0], 0);
                        state.setInstancesElement(iid, nopRecord);
                        descriptors.add(new Accept.Descriptor(iid));
                        state.setPendingInstances(pendingRequests + 1);

                        IidAcceptorsCounts accepted = new IidAcceptorsCounts(iid, ballotProposer);
                        state.setAcceptedElement(iid, accepted);
                        accepted.setReceivedRequests(bufSize);
                        accepted.setTotalRequests(bufSize);
                        accepted.setAccepted(true);

                        descriptors.add(new Accepted.Descriptor(iid));
                    }
                    descriptors.add(new Accept.Descriptor(currIid));
                    state.setPendingInstances(pendingRequests + 1);

                    IidAcceptorsCounts accepted = new IidAcceptorsCounts(currIid, ballotProposer);
                    state.setAcceptedElement(currIid, accepted);
                    accepted.setReceivedRequests(bufSize);
                    accepted.setTotalRequests(bufSize);
                    accepted.setAccepted(true);

                    descriptors.add(new Accepted.Descriptor(currIid));

                    // update the current iid
                    LOG.trace("Setting currIid to {} from {}.", currIid + 1, state.getCurrIid());
                    state.setCurrIid(currIid + 1);
                }
            }

            // Set a clean IntanceRecord for the current instance
            long currIid = state.getCurrIid();
            LOG.trace("Setting clean InstanceRecord for iid {}.", currIid);
            state.setInstancesElement(currIid,
                    new InstanceRecord(currIid, state.getBallotProposer(), state.getBufferSize()));
        }

        return descriptors;
    }

}
