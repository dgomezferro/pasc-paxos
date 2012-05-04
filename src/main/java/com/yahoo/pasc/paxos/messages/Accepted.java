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
import java.util.Arrays;
import java.util.List;

import com.yahoo.pasc.CloneableDeep;
import com.yahoo.pasc.EqualsDeep;
import com.yahoo.pasc.paxos.state.PaxosState;

public class Accepted extends PaxosMessage implements Serializable, CloneableDeep<Accepted>, EqualsDeep<Accepted>{

    private static final long serialVersionUID = 4929988247614939786L;
    
    public static class Descriptor implements PaxosDescriptor, EqualsDeep<Descriptor> {
        
        long iid;

        public Descriptor(long iid) {
            this.iid = iid;
        }
        
        @Override
        public List<PaxosMessage> buildMessages(PaxosState state) {
            return Arrays.<PaxosMessage>asList(new Accepted(state.getServerId(), state.getBallotAcceptor(), iid));
        }

        @Override
        public boolean equalsDeep(Descriptor other) {
            return this.iid == other.iid;
        }
        
    }

    int senderId;
    int ballot;
    long iid;

    public Accepted() {
    }

    public Accepted(int senderId, int ballot, long iid) {
        super();
        this.senderId = senderId;
        this.ballot = ballot;
        this.iid = iid;
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

    public long getIid() {
        return iid;
    }

    public void setIid(long iid) {
        this.iid = iid;
    }

    @Override
    public String toString() {
        return String.format("{Accepted iid %d sent from %d with ballot %d %s}", iid, senderId, ballot, super.toString());  
    }

    public Accepted cloneDeep(){
    	return new Accepted(this.senderId, this.ballot, this.iid);
    }
    
    public boolean equalsDeep(Accepted other){
    	return (this.senderId == other.senderId || this.ballot == other.ballot || this.iid == other.iid);
    }
 
}
