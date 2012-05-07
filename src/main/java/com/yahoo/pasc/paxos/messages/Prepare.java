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

import java.util.Arrays;
import java.util.List;

import com.yahoo.pasc.paxos.state.PaxosState;

public class Prepare extends PaxosMessage implements PaxosDescriptor {

    private static final long serialVersionUID = -4705441494405137950L;

    int senderId;
    int ballot;
    long maxExecutedIid;

    public Prepare(int senderId, int ballot, long maxExecutedIid) {
        this.senderId = senderId;
        this.ballot = ballot;
        this.maxExecutedIid = maxExecutedIid;
    }

    public int getSenderId() {
        return senderId;
    }

    public void setSenderId(int senderId) {
        this.senderId = senderId;
    }

    public int getBallot() {
        return ballot;
    }

    public void setBallot(int ballot) {
        this.ballot = ballot;
    }

    public long getMaxExecutedIid() {
        return maxExecutedIid;
    }

    public void setMaxExecutedIid(int maxExecutedIid) {
        this.maxExecutedIid = maxExecutedIid;
    }

    @Override
    public String toString() {
        return String.format("{Prepare sent from %d with ballot %d and maxExecutedId %d}", senderId, ballot,
                maxExecutedIid);
    }

    @Override
    public List<PaxosMessage> buildMessages(PaxosState state) {
        return Arrays.<PaxosMessage>asList(this);
    }
}
