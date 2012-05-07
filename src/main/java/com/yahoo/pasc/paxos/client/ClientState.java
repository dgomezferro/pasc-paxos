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

package com.yahoo.pasc.paxos.client;

import java.util.BitSet;

import com.yahoo.pasc.ProcessState;
import com.yahoo.pasc.paxos.messages.Request;

public class ClientState implements ProcessState {
    int clientId;
    int servers;
    int quorum;
    int connected;
    int inlineThreshold;

    long timestamp = -1;
    Request pendingRequest;
    BitSet acks = new BitSet();
    byte[] value;
    private int from;

    public ClientState(int clientId, int servers, int quorum, int inlineThreshold) {
        this.clientId = clientId;
        this.servers = servers;
        this.quorum = quorum;
        this.inlineThreshold = inlineThreshold;
    }

    public int getClientId() {
        return clientId;
    }

    public void setClientId(int clientId) {
        this.clientId = clientId;
    }

    public int getServers() {
        return servers;
    }

    public void setServers(int servers) {
        this.servers = servers;
    }

    public int getQuorum() {
        return quorum;
    }

    public void setQuorum(int quorum) {
        this.quorum = quorum;
    }

    public int getConnected() {
        return connected;
    }

    public void setConnected(int connected) {
        this.connected = connected;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public Request getPendingRequest() {
        return pendingRequest;
    }

    public void setPendingRequest(Request pendingRequest) {
        this.pendingRequest = pendingRequest;
    }

    public BitSet getAcks() {
        return acks;
    }

    public void setAcks(BitSet acks) {
        this.acks = acks;
    }

    public int getInlineThreshold() {
        return inlineThreshold;
    }

    public void setInlineThreshold(int inlineThreshold) {
        this.inlineThreshold = inlineThreshold;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

    public void setFrom(int serverId) {
        this.from = serverId;
    }

    public int getFrom() {
        return from;
    }

}
