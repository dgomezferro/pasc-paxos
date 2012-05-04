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
import java.util.Arrays;

import com.yahoo.pasc.CloneableDeep;
import com.yahoo.pasc.EqualsDeep;

public final class InstanceRecord implements Serializable, EqualsDeep<InstanceRecord>, CloneableDeep<InstanceRecord> {

    private static final long serialVersionUID = -9144513788670934858L;

    long iid;
    int ballot;
    ClientTimestamp[] clientTimestamps;
    int arraySize;

    public InstanceRecord(long iid, int ballot, int bufferSize) {
        this.iid = iid;
        this.ballot = ballot;
        clientTimestamps = new ClientTimestamp[bufferSize];
        arraySize = 0;
    }

    public InstanceRecord(long iid, int ballot, ClientTimestamp[] clientTimestamps, int arraySize) {
        super();
        this.iid = iid;
        this.ballot = ballot;
        this.clientTimestamps = clientTimestamps;
        this.arraySize = arraySize;
    }

    public long getIid() {
        return iid;
    }

    public void setIid(long iid) {
        this.iid = iid;
    }

    public int getBallot() {
        return ballot;
    }

    public void setBallot(int ballot) {
        this.ballot = ballot;
    }

    public ClientTimestamp[] getClientTimestamps() {
        return clientTimestamps;
    }

    public void setClientTimestamps(ClientTimestamp[] cts) {
        clientTimestamps = cts;
    }

    public void setClientTimestamp(ClientTimestamp ct, int index) {
        clientTimestamps[index] = ct;
    }

    public ClientTimestamp getClientTimestamp(int index) {
        return clientTimestamps[index];
    }

    public int getArraySize() {
        return arraySize;
    }

    public void setArraySize(int newSize) {
        arraySize = newSize;
    }

    @Override
    public String toString() {
        return String
                .format("{iid:%d bl:%d sz:%d array:%s}", iid, ballot, arraySize, Arrays.toString(clientTimestamps));
    }

    public InstanceRecord cloneDeep() {
        ClientTimestamp[] resClientTimestamps = new ClientTimestamp[this.clientTimestamps.length];
        for (int i = 0; i < this.arraySize; i++) {
            resClientTimestamps[i] = this.clientTimestamps[i].cloneDeep();
        }
        return new InstanceRecord(this.iid, this.ballot, resClientTimestamps, this.arraySize);
    }

    public boolean equalsDeep(InstanceRecord other) {
        if (this.iid != other.iid || this.ballot != other.ballot || this.arraySize != other.arraySize) {
            return false;
        }
        for (int i = 0; i < this.arraySize; i++) {
            if (!this.clientTimestamps[i].equalsDeep(other.clientTimestamps[i])) {
                return false;
            }
        }
        return true;
    }
}
