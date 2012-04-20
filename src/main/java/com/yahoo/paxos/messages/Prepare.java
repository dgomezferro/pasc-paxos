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

public class Prepare extends PaxosMessage {

    private static final long serialVersionUID = -4705441494405137950L;

    int senderId;
    int ballot;
    int maxLearnedIid;

    public Prepare() {
        // TODO Auto-generated constructor stub
    }

    public Prepare(int senderId, int ballot, int maxLearnedIid) {
        super();
        this.senderId = senderId;
        this.ballot = ballot;
        this.maxLearnedIid = maxLearnedIid;
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

    public int getMaxLearnedIid() {
        return maxLearnedIid;
    }

    public void setMaxLearnedIid(int maxLearnedIid) {
        this.maxLearnedIid = maxLearnedIid;
    }

    @Override
    public String toString() {
        return String.format("{Prepare sent from %d with ballot %d and maxLearnedId %d}", senderId, ballot,
                maxLearnedIid);
    }
}
