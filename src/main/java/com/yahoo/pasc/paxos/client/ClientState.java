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
import com.yahoo.pasc.paxos.messages.Hello;
import com.yahoo.pasc.paxos.messages.Request;

public class ClientState implements ProcessState {
    int clientId;
    int servers;
    int quorum;
    int connected;
    int disconnected;
    int inlineThreshold;
    TimestampMessage asyncMessages [];

    long timestamp = -1;
    Request pendingRequest;
    Hello pendingHello;
    BitSet acks = new BitSet();
//    byte[] value;
    private int from;
    ReplyStore replyStore;

    public ClientState(int servers, int quorum, int inlineThreshold, int asyncMessages) {
        this.servers = servers;
        this.quorum = quorum;
        this.inlineThreshold = inlineThreshold;
        this.asyncMessages = new TimestampMessage[asyncMessages];
        this.replyStore = new ReplyStore(servers);
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

    public int getDisconnected() {
        return disconnected;
    }

    public void setDisconnected(int disconnected) {
        this.disconnected = disconnected;
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

    public Hello getPendingHello() {
        return pendingHello;
    }

    public void setPendingHello(Hello pendingHello) {
        this.pendingHello = pendingHello;
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
//
//    public byte[] getValue() {
//        return value;
//    }
//
//    public void setValue(byte[] value) {
//        this.value = value;
//    }

    public void setFrom(int serverId) {
        this.from = serverId;
    }

    public ReplyStore getReplyStore() {
        return replyStore;
    }

    public void setReplyStore(ReplyStore replyStore) {
        this.replyStore = replyStore;
    }

    public int getFrom() {
        return from;
    }

    public TimestampMessage getAsyncMessage(long i) {
        return asyncMessages[(int)(i % asyncMessages.length)];
    }

    public void setAsyncMessage(long i, TimestampMessage m) {
        asyncMessages[(int)(i % asyncMessages.length)] = m;
    }

}
