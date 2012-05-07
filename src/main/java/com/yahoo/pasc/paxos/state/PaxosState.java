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

package com.yahoo.pasc.paxos.state;

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.pasc.ProcessState;

public class PaxosState implements ProcessState {

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(PaxosState.class);

    private static final int MAX_CLIENTS = 4096;
    // this field is just for testing. will be replaced when the learner will
    // use the state machine
    public final int REPLY_SIZE = 1;

    public PaxosState(int maxInstances, int bufferSize, int serverId, int quorum, int digestQuorum,
            int servers, int congestionWindow, int maxDigests) {
        this.maxInstances = maxInstances;
        this.instances = new InstanceRecord[maxInstances];
        this.accepted = new IidAcceptorsCounts[maxInstances];
        this.requests = new IidRequest[MAX_CLIENTS][];
        this.bufferSize = bufferSize;
        this.serverId = serverId;
        this.quorum = quorum;
        this.digestQuorum = digestQuorum;
        this.servers = servers;
        this.congestionWindow = congestionWindow;
        this.maxDigests = maxDigests;
        this.digestStore = new DigestStore[maxDigests];
        this.maxExecuted = -1;
        this.replyCache = new TimestampReply[MAX_CLIENTS];
        inProgress = new long[MAX_CLIENTS];
        Arrays.fill(inProgress, -1);
        prepared = new PreparedMessages(servers);
    }
    
    // Digests
    int digestQuorum;
    DigestStore[] digestStore;
    long firstDigestId;
    int maxDigests;
    int checkpointPeriod = 256;

    // All
    int serverId;
    int quorum;
    int servers;
    int bufferSize;

    /*
     * Proposer
     */
    int congestionWindow;
    /** Execution pending instances */
    int pendingInstances;
    IidRequest[][] requests;
    boolean isLeader;
    int ballotProposer;
    /** Size under which request are forwarded through the leader */
    int requestThreshold;
    PreparedMessages prepared;

    /*
     * Proposer & Acceptor
     */

    InstanceRecord[] instances;
    long firstInstanceId;
    int maxInstances;
    long currIid;

    /*
     * Acceptor
     */
    int ballotAcceptor;

    /*
     * Learner
     */
    IidAcceptorsCounts[] accepted;
    long maxExecuted;

    // Generate messages
    TimestampReply[] replyCache;
    long[] inProgress;
    boolean leaderReplies;
    boolean completedPhaseOne;

    // ------------------------------

    public int getBallotProposer() {
        return ballotProposer;
    }

    public void setBallotProposer(int ballotProposer) {
        this.ballotProposer = ballotProposer;
    }

    public boolean getIsLeader() {
        return isLeader;
    }

    public void setIsLeader(boolean isLeader) {
        this.isLeader = isLeader;
    }

    public InstanceRecord[] getInstances() {
        return instances;
    }

    public void setInstances(InstanceRecord[] instances) {
        this.instances = instances;
    }

    public int getBallotAcceptor() {
        return ballotAcceptor;
    }

    public void setBallotAcceptor(int ballotAcceptor) {
        this.ballotAcceptor = ballotAcceptor;
    }

    public IidAcceptorsCounts[] getAccepted() {
        return accepted;
    }

    public void setAccepted(IidAcceptorsCounts[] accepted) {
        this.accepted = accepted;
    }

    public long getReplyCacheTimestampElement(int clientId){
        TimestampReply reply = replyCache[clientId];
        if (reply != null)
            return reply.getTimestamp();
        return -1;
    }
    
    public void setReplyCacheTimestampElement(int clientId, long newVal){
    }
    
    public TimestampReply getReplyCacheElement(int clientId){
        return replyCache[clientId];
    }
    
    public void setReplyCacheElement(int clientId, TimestampReply newVal){
        replyCache[clientId] = newVal;
    }
    
    public long getInProgressElement(int clientId){
        return inProgress[clientId];
    }
    
    public void setInProgressElement(int clientId, long timestamp){
        inProgress[clientId] = timestamp;
    }

    public void setClientTimestampBufferElem(IndexIid index, ClientTimestamp ct) {
        instances[(int) (index.getIid() % maxInstances)].setClientTimestamp(ct, index.getIndex());
    }

    public ClientTimestamp getClientTimestampBufferElem(IndexIid index) {
        return instances[(int) (index.getIid() % maxInstances)].getClientTimestamp(index.getIndex());
    }

    public int getInstanceBufferSize(long iid) {
        return instances[(int) (iid % maxInstances)].getArraySize();
    }

    public void setInstanceBufferSize(long iid, int newSize) {
        instances[(int) (iid % maxInstances)].setArraySize(newSize);
    }

    public InstanceRecord getInstancesElement(long iid) {
        return instances[(int) (iid % maxInstances)];
    }

    public long getInstancesIid (long iid) {
        return instances[(int) (iid % maxInstances)].getIid();
    }

    public void setInstancesElement(long iid, InstanceRecord instancesElement) {
        this.instances[(int) (iid % maxInstances)] = instancesElement;
    }

    public int getInstancesBallot(long iid){
        return instances[(int) (iid % maxInstances)].getBallot();
    }

    public IidAcceptorsCounts getAcceptedElement(long iid) {
        return accepted[(int) (iid % maxInstances)];
    }

    public boolean getIsAcceptedElement(long iid) {
        return accepted[(int) (iid % maxInstances)].isAccepted();
    }

    public void setAcceptedElement(long iid, IidAcceptorsCounts acceptedElement) {
        this.accepted[(int) (iid % maxInstances)] = acceptedElement;
    }

    public long getCurrIid() {
        return currIid;
    }

    public void setCurrIid(long firstInstancesElement) {
        this.currIid = firstInstancesElement;
    }

    public long getReceivedRequestIid(ClientTimestamp element) {
        IidRequest[] clientArray = requests[element.clientId];
        if (clientArray == null || clientArray.length == 0) {
            return -1;
        }
        IidRequest request = clientArray[(int) (element.timestamp % maxInstances)];
        if (request != null)
            return request.getIid();
        return -1;
    }

    public void setReceivedRequestIid(ClientTimestamp element, long value) {
    }

    public IidRequest getReceivedRequest(ClientTimestamp element) {
        IidRequest[] clientArray = requests[element.clientId];
        if (clientArray == null || clientArray.length == 0) {
            return null;
        }
        return clientArray[(int) (element.timestamp % maxInstances)];
    }

    public void setReceivedRequest(ClientTimestamp element, IidRequest value) {
        IidRequest[] clientArray = requests[element.clientId];
        if (clientArray == null || clientArray.length == 0) {
            clientArray = new IidRequest[maxInstances];
            requests[element.clientId] = clientArray;
        }
        clientArray[(int) (element.timestamp % maxInstances)] = value;
    }

    public int getMaxInstances() {
        return maxInstances;
    }

    public void setMaxInstances(int maxInstances) {
        this.maxInstances = maxInstances;
    }

    public int getServerId() {
        return serverId;
    }

    public void setServerId(int serverId) {
        this.serverId = serverId;
    }

    public int getQuorum() {
        return quorum;
    }

    public void setQuorum(int quorum) {
        this.quorum = quorum;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    public int getServers() {
        return servers;
    }

    public void setServers(int servers) {
        this.servers = servers;
    }

    public int getCongestionWindow() {
        return congestionWindow;
    }

    public void setCongestionWindow(int congestionWindow) {
        this.congestionWindow = congestionWindow;
    }

    public int getPendingInstances() {
        return pendingInstances;
    }

    public void setPendingInstances(int pendingInstances) {
        this.pendingInstances = pendingInstances;
    }

//    public int getClientTimestampBufferSize() {
//        return clientTimestampBufferSize;
//    }
//
//    public void setClientTimestampBufferSize(int clientTimestampBufferSize) {
//        this.clientTimestampBufferSize = clientTimestampBufferSize;
//    }

//    public ClientTimestamp getClientTimestampBufferElement(int index) {
//        return clientTimestampBuffer[index];
//    }
//
//    public void setClientTimestampBufferElement(int index, ClientTimestamp clientTimestampBuffer) {
//        this.clientTimestampBuffer[index] = clientTimestampBuffer;
//    }
//
//    public ClientTimestamp[] getClientTimestampBuffer() {
//        return clientTimestampBuffer;
//    }
//
//    public void setClientTimestampBuffer(ClientTimestamp[] clientTimestampBuffer) {
//        this.clientTimestampBuffer = clientTimestampBuffer;
//    }

    public int getRequestThreshold() {
        return requestThreshold;
    }

    public void setRequestThreshold(int requestThreshold) {
        this.requestThreshold = requestThreshold;
    }

    public DigestStore[] getDigestStore() {
        return digestStore;
    }

    public void setDigestStore(DigestStore[] digestStore) {
        this.digestStore = digestStore;
    }

    public DigestStore getDigestStoreElement(long digestId) {
        return digestStore[(int) (digestId % maxDigests)];
    }

    public void setDigestStoreElement(long digestId, DigestStore digestStoreElement) {
        this.digestStore[(int) (digestId % maxDigests)] = digestStoreElement;
    }

    public int getCheckpointPeriod() {
        return checkpointPeriod;
    }

    public void setCheckpointPeriod(int checkpointPeriod) {
        this.checkpointPeriod = checkpointPeriod;
    }

    public long getMaxExecuted() {
        return maxExecuted;
    }

    public void setMaxExecuted(long maxExecuted) {
        this.maxExecuted = maxExecuted;
    }

    public long getFirstDigestId() {
        return firstDigestId;
    }

    public void setFirstDigestId(long firstDigestId) {
        this.firstDigestId = firstDigestId;
    }

    public int getMaxDigests() {
        return maxDigests;
    }

    public void setMaxDigests(int maxDigests) {
        this.maxDigests = maxDigests;
    }

    public long getFirstInstanceId() {
        return firstInstanceId;
    }

    public void setFirstInstanceId(long firstInstanceId) {
        this.firstInstanceId = firstInstanceId;
    }

    public int getDigestQuorum() {
        return digestQuorum;
    }

    public void setDigestQuorum(int digestQuorum) {
        this.digestQuorum = digestQuorum;
    }

    public boolean getLeaderReplies() {
        return leaderReplies;
    }

    public void setLeaderReplies(boolean leaderReplies) {
        this.leaderReplies = leaderReplies;
    }

    public PreparedMessages getPrepared() {
        return prepared;
    }

    public void setPrepared(PreparedMessages prepared) {
        this.prepared = prepared;
    }

    public void setCompletedPhaseOne(boolean completedPhaseOne) {
        this.completedPhaseOne = completedPhaseOne;
    }

    public boolean getCompletedPhaseOne() {
        return this.completedPhaseOne;
    }
}
