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
import com.yahoo.pasc.Message;
import com.yahoo.pasc.paxos.state.PaxosState;

public class DigestToSM extends PaxosMessage implements Serializable, CloneableDeep<DigestToSM>, EqualsDeep<DigestToSM>{

    private static final long serialVersionUID = -3781061394615967506L;
    
    public static class Descriptor implements PaxosDescriptor, EqualsDeep<Descriptor> {

        private long digest;
        
        public Descriptor(long digest) {
            this.digest = digest;
        }
        
        @Override
        public List<PaxosMessage> buildMessages(PaxosState state) {
            return Arrays.<PaxosMessage>asList(new DigestToSM(digest));
        }

        @Override
        public boolean equalsDeep(Descriptor other) {
            return this.digest == other.digest;
        }
        
    }

    long digest;

    public DigestToSM(long digest) {
        this.digest = digest;
    }

    public long getDigest() {
        return digest;
    }

    @Override
    public String toString() {
        return String.format("{Digest %s sent to state machine}", digest);  
    }
    
    public DigestToSM cloneDeep(){
    	return new DigestToSM(digest); 
    }
    
    public boolean equalsDeep (DigestToSM other){
    	return other != null && this.digest == other.digest;
    }

    @Override
    protected boolean verify() {
        return true;
    }

    @Override
    public void storeReplica(Message m) {
    }
}
