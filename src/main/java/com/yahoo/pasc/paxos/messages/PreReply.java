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
import java.util.Arrays;

import com.yahoo.pasc.CloneableDeep;
import com.yahoo.pasc.EqualsDeep;
import com.yahoo.pasc.Message;

public class PreReply extends PaxosMessage implements Serializable, CloneableDeep<PreReply>, EqualsDeep<PreReply> {

    private static final long serialVersionUID = 4929988247614939786L;

    int clientId;
    long timestamp;
    long iid;
    byte[] reply;
    Long digest;

    public PreReply() {
    }

    public PreReply(int clientId, long timestamp, byte[] reply) {
        super();
        this.clientId = clientId;
        this.timestamp = timestamp;
        this.reply = reply;
    }

    public PreReply(int clientId, long timestamp, byte[] reply, Long digest, long iid) {
        this(clientId, timestamp, reply);
        this.digest = digest;
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

    public byte[] getReply() {
        return reply;
    }

    public void setReply(byte[] reply) {
        this.reply = reply;
    }

    public Long getDigest() {
        return digest;
    }

    public void setDigest(Long digest) {
        this.digest = digest;
    }

    public long getIid() {
        return iid;
    }

    public void setIid(long iid) {
        this.iid = iid;
    }

    @Override
    public String toString() {
        return String.format("{PreReply %s <%d,%d>}", Arrays.toString(reply), clientId, timestamp);
    }

    public PreReply cloneDeep() {
        byte[] resArray = new byte[this.reply.length];
        for (int i = 0; i < this.reply.length; i++) {
            resArray[i] = this.reply[i];
        }
        return new PreReply(this.clientId, this.timestamp, resArray, this.digest, this.iid);
    }

    public boolean equalsDeep(PreReply other) {
        for (int i = 0; i < this.reply.length; i++) {
            if (other.reply[i] != this.reply[i]) {
                return false;
            }
        }
        if (this.digest == null) {
            if (other.digest != null) return false;
        } else if (!this.digest.equals(other.digest)) {
            return false;
        }
        return (this.clientId == other.clientId && this.timestamp == other.timestamp && this.iid == other.iid);
    }

    @Override
    protected boolean verify() {
        return true;
    }
    
    @Override
    public void storeReplica(Message m) {
    }
}
