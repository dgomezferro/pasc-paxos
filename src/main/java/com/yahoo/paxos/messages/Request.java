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

import java.io.Serializable;
import java.util.Arrays;

import com.yahoo.pasc.CloneableDeep;
import com.yahoo.pasc.EqualsDeep;
import com.yahoo.pasc.MessageDescriptor;

public class Request extends PaxosMessage implements Serializable, EqualsDeep<Request>, CloneableDeep<Request>, MessageDescriptor {

    private static final long serialVersionUID = 1111659280353033430L;

    int clientId;
    long timestamp;
    byte[] request;

    public Request() {
    }

    public Request(int clientId, long timestamp, byte[] request) {
        super();
        this.clientId = clientId;
        this.timestamp = timestamp;
        this.request = request;
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

    @Override
    public String toString() {
        return String.format("{Request %s sent from %d at %d %s}", Arrays.toString(request), clientId, timestamp, super.toString());  
    }
    
    public Request cloneDeep (){
    	byte [] resArray = new byte[this.request.length];
    	System.arraycopy(request, 0, resArray, 0, request.length);
    	return new Request(this.clientId, this.timestamp, resArray);
    }
    
    public boolean equalsDeep(Request other){
		if (!Arrays.equals(other.request, this.request)) {
			return false;
		}
    	return (this.clientId == other.clientId && this.timestamp == other.timestamp);
    }
}
