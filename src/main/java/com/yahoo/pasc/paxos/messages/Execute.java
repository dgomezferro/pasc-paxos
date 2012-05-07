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

package com.yahoo.pasc.paxos.messages;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.yahoo.pasc.CloneableDeep;
import com.yahoo.pasc.EqualsDeep;
import com.yahoo.pasc.Message;
import com.yahoo.pasc.paxos.state.ClientTimestamp;
import com.yahoo.pasc.paxos.state.InstanceRecord;
import com.yahoo.pasc.paxos.state.PaxosState;

public class Execute extends PaxosMessage implements Serializable, CloneableDeep<Execute>, EqualsDeep<Execute> {
    private static final long serialVersionUID = 5929988247614939786L;

    public static class Descriptor implements PaxosDescriptor, EqualsDeep<Descriptor> {

        private long iid;

        public Descriptor(long iid) {
            this.iid = iid;
        }

        @Override
        public List<PaxosMessage> buildMessages(PaxosState state) {
            List<PaxosMessage> messages = new ArrayList<PaxosMessage>();
            boolean generateDigest = false;
            if (iid % state.getCheckpointPeriod() == 0) {
                generateDigest = true;
            }
            InstanceRecord instanceRec = state.getInstancesElement(iid);
            if (instanceRec != null) {
                ClientTimestamp[] cts = instanceRec.getClientTimestamps();
                int size = instanceRec.getArraySize();
                for (int i = 0; i < size; ++i) {
                    ClientTimestamp ct = cts[i];
                    int clientId = ct.getClientId();
                    long timestamp = ct.getTimestamp();
                    boolean last = i == size - 1;
                    messages.add(new Execute(clientId, timestamp, state.getReceivedRequest(ct).getRequest(),
                            generateDigest && last, iid));
                }
            }
            return messages;
        }

        @Override
        public boolean equalsDeep(Descriptor other) {
            return this.iid == other.iid;
        }

    }

    int clientId;
    long timestamp;
    boolean generateDigest;
    long iid;
    byte[] request;

    public Execute() {
    }

    public Execute(int clientId, long timestamp, byte[] request, boolean generateDigest, long iid) {
        super();
        this.clientId = clientId;
        this.timestamp = timestamp;
        this.request = request;
        this.generateDigest = generateDigest;
        this.iid = iid;
    }

    public int getClientId() {
        return clientId;
    }

    public void setClientId(int clientId) {
        this.clientId = clientId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public byte[] getRequest() {
        return request;
    }

    public void setRequest(byte[] request) {
        this.request = request;
    }

    public boolean isGenerateDigest() {
        return generateDigest;
    }

    public void setGenerateDigest(boolean generateDigest) {
        this.generateDigest = generateDigest;
    }

    public long getIid() {
        return iid;
    }

    public void setIid(long iid) {
        this.iid = iid;
    }

    @Override
    public String toString() {
        String requestStr = Arrays.toString(request);
        if (requestStr.length() > 20) {
            requestStr = requestStr.substring(0, 20) + " ... ]";
        }
        return String.format("{Execute %s sent from %d at %d %s (iid %d)}", requestStr, clientId, timestamp,
                super.toString(), iid);
    }

    public Execute cloneDeep() {
        byte[] resArray = new byte[this.request.length];
        System.arraycopy(this.request, 0, resArray, 0, this.request.length);
        return new Execute(this.clientId, this.timestamp, resArray, this.generateDigest, this.iid);
    }

    public boolean equalsDeep(Execute other) {
        if (!Arrays.equals(this.request, other.request))
            return false;
        return (this.clientId == other.clientId && this.timestamp == other.timestamp && this.generateDigest == other.generateDigest && this.iid == other.iid);
    }

    @Override
    protected boolean verify() {
        return true;
    }

    @Override
    public void storeReplica(Message m) {
    }
}
