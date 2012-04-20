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

package com.yahoo.paxos.messages;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import com.yahoo.pasc.CloneableDeep;
import com.yahoo.pasc.EqualsDeep;
import com.yahoo.paxos.state.ClientTimestamp;
import com.yahoo.paxos.state.IidRequest;
import com.yahoo.paxos.state.InstanceRecord;
import com.yahoo.paxos.state.PaxosState;

public class Accept extends PaxosMessage implements Serializable, CloneableDeep<Accept>, EqualsDeep<Accept> {

    private static final long serialVersionUID = -3781061394615967506L;

    public static class Descriptor implements PaxosDescriptor, EqualsDeep<Descriptor> {

        private long iid;

        public Descriptor(long iid) {
            this.iid = iid;
        }

        @Override
        public List<PaxosMessage> buildMessages(PaxosState state) {
            InstanceRecord instance = state.getInstancesElement(iid);
            ClientTimestamp cts[] = instance.getClientTimestamps();
            int threshold = state.getRequestThreshold();
            int size = instance.getArraySize();
            byte[][] requests = null;
            for (int i = 0; i < size; ++i) {
                IidRequest request = state.getReceivedRequest(cts[i]);
                if (request.getRequest().length <= threshold) {
                    if (requests == null) {
                        requests = new byte[size][];
                    }
                    requests[i] = request.getRequest();
                }
            }
            return Arrays.<PaxosMessage> asList(new Accept(state.getServerId(), instance, requests));
        }

        @Override
        public boolean equalsDeep(Descriptor other) {
            return this.iid == other.iid;
        }

    }

    int senderId;
    InstanceRecord instance;
    byte[][] requests;

    public Accept() {
    }

    public Accept(int senderId, InstanceRecord instance, byte[][] requests) {
        this.senderId = senderId;
        this.instance = instance;
        this.requests = requests;
    }

    public Accept(int senderId, long iid, int ballot, ClientTimestamp[] values, int arraySize) {
        super();
        this.senderId = senderId;
        this.instance = new InstanceRecord(iid, ballot, values, arraySize);
    }

    public int getSenderId() {
        return senderId;
    }

    public void setSenderId(int senderId) {
        this.senderId = senderId;
    }

    public InstanceRecord getInstance() {
        return instance;
    }

    public int getBallot() {
        return instance.getBallot();
    }

    public void setBallot(int ballot) {
        instance.setBallot(ballot);
    }

    public long getIid() {
        return instance.getIid();
    }

    public void setIid(long iid) {
        instance.setIid(iid);
    }

    public ClientTimestamp[] getValues() {
        return instance.getClientTimestamps();
    }

    public void setValues(ClientTimestamp[] values) {
        instance.setClientTimestamps(values);
    }

    public int getArraySize() {
        return instance.getArraySize();
    }

    public void setArraySize(int size) {
        instance.setArraySize(size);
    }

    public byte[][] getRequests() {
        return requests;
    }

    public void setRequests(byte[][] requests) {
        this.requests = requests;
    }

    @Override
    public String toString() {
        return String.format("{Accept %s sent from %d with ballot %d for iid %d with requests %s {%s}}",
                Arrays.toString(instance.getClientTimestamps()), senderId, instance.getBallot(), instance.getIid(), 
                Arrays.toString(requests), super.toString());
    }

    public Accept cloneDeep() {
        byte[][] requests = null;
        if (this.requests != null) {
            int size = this.requests.length;
            requests = new byte[size][];
            for (int i = 0; i < size; i++) {
                byte[] request = this.requests[i];
                if (request != null) {
                    requests[i] = new byte[request.length];
                    System.arraycopy(request, 0, requests[i], 0, request.length);
                }
            }
        }
        return new Accept(this.senderId, instance.cloneDeep(), requests);
    }

    public boolean equalsDeep(Accept other) {
        return (this.senderId == other.senderId && this.instance.equalsDeep(other.instance));
    }
}
