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

package com.yahoo.paxos.handlers.proposer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.pasc.Message;
import com.yahoo.paxos.handlers.PaxosHandler;
import com.yahoo.paxos.handlers.acceptor.AcceptorAccept;
import com.yahoo.paxos.messages.Accept;
import com.yahoo.paxos.messages.Accepted;
import com.yahoo.paxos.messages.PaxosDescriptor;
import com.yahoo.paxos.messages.Reply;
import com.yahoo.paxos.messages.Request;
import com.yahoo.paxos.state.ClientTimestamp;
import com.yahoo.paxos.state.IidAcceptorsCounts;
import com.yahoo.paxos.state.IidRequest;
import com.yahoo.paxos.state.IndexIid;
import com.yahoo.paxos.state.InstanceRecord;
import com.yahoo.paxos.state.PaxosState;

public class ProposerRequest extends PaxosHandler<Request> {

    private static final Logger LOG = LoggerFactory.getLogger(ProposerRequest.class);

    @Override
    public boolean guardPredicate(Message receivedMessage) {
        return receivedMessage instanceof Request;
    }
    
    @Override
    public List<PaxosDescriptor> processMessage(Request message, PaxosState state) {
        long firstInstanceId = state.getFirstInstanceId();
        int maxInstances = state.getMaxInstances();

        int clientId = message.getClientId();
        long timestamp = message.getTimestamp();
        
    	// TODO if a request is received twice, we should start a timer 
    	
        ClientTimestamp ct = new ClientTimestamp(clientId, timestamp);

        List<PaxosDescriptor> descriptors = null;
        
        // check reply cache
        long repTs = state.getReplyCacheTimestampElement(clientId);
        if (repTs >= timestamp){
//            LOG.trace("We hit the reply cache for client {} with timestamp {}.",
//                    clientId, timestamp);
            if (state.getIsLeader()){
                return null;
            } else {
                return Arrays.<PaxosDescriptor>asList(new Reply.Descriptor(clientId));
            }
        }
        
        IidRequest request;
        long requestIid = state.getReceivedRequestIid(ct);
        if (requestIid < firstInstanceId) {
//            LOG.trace("Creating new request (Old one:{})", request);
            request = new IidRequest(message.getRequest());
            state.setReceivedRequest(ct, request);
        } else {
            request = state.getReceivedRequest(ct);
            if (request.getRequest() == null) {
    //            LOG.trace("Appending request to accepted element");
                request.setRequest(message.getRequest());
                
                descriptors = new ArrayList<PaxosDescriptor>(4);
                AcceptorAccept.checkAccept(request.getIid(), state, descriptors);
            } else {
    //            LOG.error("Got a resubmitted request or too new request. Discard message {}. FirstDigestID {}.", message, state.getFirstDigestId());
    //            LOG.error("CurrentId: {} RequestId: {} FirstIid: {}", new Object[] {state.getCurrIid(), request.getIid(), state.getFirstInstanceId()});
                return null;
            }
        }
        
        if (state.getIsLeader() && timestamp > state.getInProgressElement(clientId)) {
        	state.setInProgressElement(clientId, timestamp);
        	long iid = state.getCurrIid();

            if (iid == firstInstanceId + maxInstances) {
                LOG.warn("Ignoring request, must process more digests before proceeding. FirstIid {} ", firstInstanceId);
                return null;
            }

        	int bufSize = state.getInstanceBufferSize(iid);
        	state.setClientTimestampBufferElem(new IndexIid(bufSize, iid), ct);
        	bufSize ++;
        	state.setInstanceBufferSize(iid, bufSize);
        	
        	int pendingRequests = state.getPendingInstances();
        	int batchSize = state.getBufferSize();
        	            
            if (pendingRequests < state.getCongestionWindow() || bufSize >= batchSize) {

//                LOG.trace("Sending batch. messages: {} batchSize {}", bufSize, batchSize);
                if (descriptors == null)
                    descriptors = new ArrayList<PaxosDescriptor>(2);
                descriptors.add(new Accept.Descriptor(iid));
                state.setPendingInstances(pendingRequests + 1);

                IidAcceptorsCounts accepted = new IidAcceptorsCounts(iid);
                state.setAcceptedElement(iid, accepted);
                accepted.setReceivedRequests(bufSize);
                accepted.setTotalRequests(bufSize);
                accepted.setAccepted(true);
                
                descriptors.add(new Accepted.Descriptor(iid));
                
            	iid++;
            	if (iid > firstInstanceId + maxInstances) {
            	    LOG.error("Reached the end of instances' buffer. Cannot progress." +
            	    		"iid: {} firstInstanceId: {} maxInstances: {}", new Object[] {iid, firstInstanceId, maxInstances} );
//            	    System.err.println("Exception Bad bad bad");
//            	    System.exit(-1);
            	    return null;
            	}
            	state.setCurrIid(iid);
                InstanceRecord nextInstance = new InstanceRecord(iid, state.getBallotProposer(), batchSize);
                state.setInstancesElement(iid, nextInstance);

//                state.setLearnedElement(iid, false);
//                int instancesSize = state.getInstancesSize();
//                instancesSize++;
//                state.setInstancesSize(instancesSize);
            } else {
//                LOG.trace("Not yet enough messages to form a batch. " +
//                		"pending: {} messages: {} batchSize {}", new Object[] {pendingRequests, bufSize, batchSize});
            }
        }
        
        return descriptors;
    }

    public static void decrementPending(PaxosState state, List<PaxosDescriptor> descriptors) {
        int pendingRequests = state.getPendingInstances();
        pendingRequests--;
        state.setPendingInstances(pendingRequests);
        
        checkSubmit(state, descriptors, null);
        
    }

    private static void checkSubmit(PaxosState state, List<PaxosDescriptor> descriptors, Request msg) {
        long iid = state.getCurrIid();
        int bufSize = state.getInstanceBufferSize(iid);
        int pendingRequests = state.getPendingInstances();

        if (msg != null) {
            int clientId = msg.getClientId();
            long timestamp = msg.getTimestamp();
            
            ClientTimestamp ct = new ClientTimestamp(clientId, timestamp);
            
    
            state.setClientTimestampBufferElem(new IndexIid(bufSize, iid), ct);
            bufSize ++;
            state.setInstanceBufferSize(iid, bufSize);
        }
        
        int batchSize = state.getBufferSize();
        long firstInstanceId = state.getFirstInstanceId();
        int maxInstances = state.getMaxInstances();
        
        boolean notEmpty = bufSize > 0;
        boolean windowSpace = pendingRequests < state.getCongestionWindow();
        boolean fullBatch = bufSize >= batchSize;
        if ((notEmpty && windowSpace) || fullBatch) {
            pendingRequests++;

//            LOG.trace("Sending batch. messages: {} batchSize {}", bufSize, batchSize);
            descriptors.add(new Accept.Descriptor(iid));
            state.setPendingInstances(pendingRequests);

            IidAcceptorsCounts accepted = new IidAcceptorsCounts(iid);
            state.setAcceptedElement(iid, accepted);
            accepted.setReceivedRequests(bufSize);
            accepted.setTotalRequests(bufSize);
            accepted.setAccepted(true);
            
            descriptors.add(new Accepted.Descriptor(iid));
            
            iid++;
            if (iid > firstInstanceId + maxInstances) {
                LOG.error("Reached the end of instances' buffer. Cannot progress." +
                        "iid: {} firstInstanceId: {} maxInstances: {}", new Object[] {iid, firstInstanceId, maxInstances} );
                return;
            }
            state.setCurrIid(iid);
            InstanceRecord nextInstance = new InstanceRecord(iid, state.getBallotProposer(), batchSize);
            state.setInstancesElement(iid, nextInstance);
        } else {
//            LOG.trace("Not yet enough messages to form a batch. " +
//                    "pending: {} messages: {} batchSize {}", new Object[] {pendingRequests, bufSize, batchSize});
        }
    }

}