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

import java.io.Serializable;

import com.yahoo.pasc.CloneableDeep;
import com.yahoo.pasc.EqualsDeep;

public final class IidAcceptorsCounts implements Serializable, EqualsDeep<IidAcceptorsCounts>, CloneableDeep<IidAcceptorsCounts> {

    private static final long serialVersionUID = 7587497611602103466L;

    long iid;
    int ballot;
    int acceptors;
    int receivedRequests;	// actual number of different requests received so far
    int totalRequests;		// number of requests with the same instance id (in the same batch from the leader)
    boolean accepted;		// true if receivedRequests == totalRequests

    public IidAcceptorsCounts(long iid, int ballot) {
        this.acceptors = 0;
        this.iid = iid;
        this.ballot = ballot;
    }

    public long getIid() {
        return iid;
    }

    public void setIid(long iid) {
        this.iid = iid;
    }

    public int getAcceptors() {
        return acceptors;
    }

    public void setAcceptors(int acceptors) {
        this.acceptors = acceptors;
    }

    public void setAcceptor(int acceptor) {
        acceptors = acceptors | (1 << acceptor);
    }

    public int getCardinality(int servers) {
        int count = 0;
        for (int i = 0; i < servers; ++i) {
            if ((acceptors & (1 << i)) != 0)
                ++count;
        }
        return count;
    }

    public boolean isAccepted() {
        return accepted;
    }

    public void setAccepted(boolean accepted) {
        this.accepted = accepted;
    }

    public int getReceivedRequests() {
        return receivedRequests;
    }

    public void setReceivedRequests(int receivedRequests) {
        this.receivedRequests = receivedRequests;
    }

    public int getTotalRequests() {
        return totalRequests;
    }

    public void setTotalRequests(int totalRequests) {
        this.totalRequests = totalRequests;
    }

    public int getBallot() {
        return ballot;
    }

    public void setBallot(int ballot) {
        this.ballot = ballot;
    }

    public IidAcceptorsCounts cloneDeep() {
        IidAcceptorsCounts res = new IidAcceptorsCounts(this.iid, this.ballot);
        res.acceptors = this.acceptors;
        res.accepted = this.accepted;
        res.receivedRequests = this.receivedRequests;
        res.totalRequests = this.totalRequests;
        return res;
    }

    public boolean equalsDeep(IidAcceptorsCounts other) {
        return (this.iid == other.iid) && (this.ballot == other.ballot) && (this.acceptors == other.acceptors)
                && (this.accepted == other.accepted) && (this.receivedRequests == other.receivedRequests)
                && (this.totalRequests == other.totalRequests);
    }
}
