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
import com.yahoo.pasc.paxos.messages.Prepared;

public final class PreparedMessages implements Serializable, EqualsDeep<PreparedMessages>, CloneableDeep<PreparedMessages> {

    private static final long serialVersionUID = 7587497611602103466L;

    Prepared[] messages;
    
    public PreparedMessages(int acceptorsNumber) {
    	messages = new Prepared[acceptorsNumber];
    }

    public int getCardinality(int servers) {
        int count = 0;
        for (int i = 0; i < servers; ++i) {
            if (messages[i] != null)
                ++count;
        }
        return count;
    }
    
    public void setPreparedMessage (Prepared message, int senderId){
    	messages[senderId] = message; 
    }
    
    public Prepared[] getPreparedMessages (){
    	return messages;
    }
    
    public void clear(){
    	for (int i = 0; i < messages.length; i++){
    		messages[i] = null;
    	}
    }

    public PreparedMessages cloneDeep() {
        PreparedMessages res = new PreparedMessages(this.messages.length);
        for (int i = 0; i < this.messages.length; i++){
            if (this.messages[i] != null) {
                res.messages[i] = this.messages[i].cloneDeep();
            }
        }
        return res;
    }

    public boolean equalsDeep(PreparedMessages other) {
        if (other.messages.length != this.messages.length){
        	return false;
        }
        for (int i = 0; i < this.messages.length; i++){
            if (this.messages[i] == null) {
                if (other.messages[i] != null) {
                    return false;
                }
            } else {
            	if(!this.messages[i].equalsDeep(other.messages[i])){
            		return false;
            	}
            }
        }
        return true;
    }
}
