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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.pasc.CloneableDeep;
import com.yahoo.pasc.EqualsDeep;
import com.yahoo.pasc.paxos.state.PaxosState;
import com.yahoo.pasc.paxos.state.TimestampReply;

public class Reply extends PaxosMessage implements Serializable, CloneableDeep<Reply>, EqualsDeep<Reply> {

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(PaxosMessage.class);

    private static final long serialVersionUID = 4929988247614939786L;

    public static class Descriptor implements PaxosDescriptor, EqualsDeep<Descriptor> {
        @SuppressWarnings("unused")
        private static final Logger LOG = LoggerFactory.getLogger(Descriptor.class);

        private int clientId;

        public Descriptor(int clientId) {
            this.clientId = clientId;
        }

        @Override
        public List<PaxosMessage> buildMessages(PaxosState state) {
            List<PaxosMessage> messages = new ArrayList<PaxosMessage>();
            TimestampReply tr = state.getReplyCacheElement(clientId);
            if (tr != null) {
                messages.add(new Reply(clientId, state.getServerId(), tr.getTimestamp(), tr.getReply()));
            } else {
                // LOG.trace("Reply cache for client {} is null", clientId);
            }
            return messages;
        }

        @Override
        public boolean equalsDeep(Descriptor other) {
            return this.clientId == other.clientId;
        }

    }

    int serverId;
    int clientId;
    long timestamp;
    byte[] reply;

    public Reply() {
    }

    public Reply(int clientId, int serverId, long timestamp, byte[] reply) {
        super();
        this.serverId = serverId;
        this.clientId = clientId;
        this.timestamp = timestamp;
        this.reply = reply;
    }

    public int getServerId() {
        return serverId;
    }

    public void setServerId(int serverId) {
        this.serverId = serverId;
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

    public byte[] getValue() {
        return reply;
    }

    public void setValue(byte[] value) {
        this.reply = value;
    }

    @Override
    public String toString() {
        return String.format("{Reply %s <%d,%d> from %d}", Arrays.toString(reply), clientId, timestamp, serverId);
    }

    @Override
    public Reply cloneDeep() {
        byte[] resArray = new byte[this.reply.length];
        System.arraycopy(this.reply, 0, resArray, 0, resArray.length);
        return new Reply(this.clientId, this.serverId, this.timestamp, resArray);
    }

    @Override
    public boolean equalsDeep(Reply other) {
        if (!Arrays.equals(other.reply, this.reply)) {
            return false;
        }
        return (this.clientId == other.clientId && this.serverId == other.serverId && this.timestamp == other.timestamp);
    }
}
