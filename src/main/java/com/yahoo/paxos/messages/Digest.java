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
import java.util.List;

import com.yahoo.pasc.CloneableDeep;
import com.yahoo.pasc.EqualsDeep;
import com.yahoo.paxos.state.PaxosState;

public class Digest extends PaxosMessage implements Serializable, CloneableDeep<Digest>, EqualsDeep<Digest>{

    private static final long serialVersionUID = -3781061394615967506L;
    
    public static class Descriptor implements PaxosDescriptor, EqualsDeep<Descriptor> {

        private long digestId;
        private long digest;
        
        public Descriptor(long digestId, long digest) {
            this.digestId = digestId;
            this.digest = digest;
        }
        
        @Override
        public List<PaxosMessage> buildMessages(PaxosState state) {
            return Arrays.<PaxosMessage>asList(new Digest(state.getServerId(), digestId, digest));
        }

        @Override
        public boolean equalsDeep(Descriptor other) {
            return this.digestId == other.digestId && this.digest == other.digest;
        }
        
    }

    int senderId;
    long digestId;
    long digest;

    public Digest() {
    }

    public Digest(int senderId, long digestId, long digest) {
        this.senderId = senderId;
        this.digestId = digestId;
        this.digest = digest;
    }

    public int getSenderId() {
        return senderId;
    }

    public void setSenderId(int senderId) {
        this.senderId = senderId;
    }
    
    public long getDigestId() {
        return digestId;
    }

    public void setDigestId(long digestId) {
        this.digestId = digestId;
    }

    public long getDigest() {
        return digest;
    }

    public void setDigest(long digest) {
        this.digest = digest;
    }

    @Override
    public String toString() {
        return String.format("{Digest %s sent from %d for iid %d}", digest, senderId, digestId);  
    }
    
    public Digest cloneDeep(){
    	return new Digest(this.senderId, digestId, digest); 
    }
    
    public boolean equalsDeep (Digest other){
    	return other != null && this.senderId == other.senderId && this.digestId == other.digestId && this.digest == other.digest;
    }
}
