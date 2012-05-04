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

import com.yahoo.pasc.CloneableDeep;
import com.yahoo.pasc.EqualsDeep;

public class ClientTimestamp implements Comparable<ClientTimestamp>, EqualsDeep<ClientTimestamp>, CloneableDeep<ClientTimestamp> {
    int clientId;
    long timestamp;

    public ClientTimestamp() {
        // TODO Auto-generated constructor stub
    }

    public ClientTimestamp(int clientId, long timestamp) {
        super();
        this.clientId = clientId;
        this.timestamp = timestamp;
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

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof ClientTimestamp)) return false;
        ClientTimestamp ct = (ClientTimestamp) obj;
        return this.clientId == ct.clientId && this.timestamp == ct.timestamp;
    }
    
    @Override
    public int hashCode() {
        return 37 * clientId + (int)(timestamp ^ (timestamp >>> 32));
    }

    @Override
    public int compareTo(ClientTimestamp o) {
        int diff = (int) (this.timestamp - o.timestamp);
        if (diff != 0) return diff;
        return this.clientId - o.clientId;
    }
    
    public ClientTimestamp cloneDeep(){
    	return new ClientTimestamp(this.clientId, this.timestamp);
    }
    
    public boolean equalsDeep (ClientTimestamp other){
        return this.clientId == other.clientId && this.timestamp == other.timestamp;
    }
    
    @Override
    public String toString() {
        return String.format("<%d,%d>", clientId, timestamp);
    }
}
