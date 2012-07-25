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

import com.yahoo.pasc.CloneableDeep;
import com.yahoo.pasc.EqualsDeep;

public class ControlMessage extends PaxosMessage implements Serializable, EqualsDeep<ControlMessage>, CloneableDeep<ControlMessage> {

    private static final long serialVersionUID = 1111659280353033430L;

    int clientId;
    byte[] controlMessage;

    public ControlMessage() {
    }

    public ControlMessage(int clientId, byte[] controlMessage) {
        super();
        this.clientId = clientId;
        this.controlMessage = controlMessage;
    }

    public int getClientId() {
        return clientId;
    }

    public void setClientId(int clientId) {
        this.clientId = clientId;
    }

    public byte[] getControlMessage() {
        return controlMessage;
    }

    public void setControlMessage(byte[] controlMessage) {
        this.controlMessage = controlMessage;
    }

    @Override
    public String toString() {
        String requestStr = Arrays.toString(controlMessage);
        if (requestStr.length() > 20) {
            requestStr = requestStr.substring(0, 20) + " ... ]";
        }
        return String.format("{ControlMessage %s sent from %d %s}", requestStr, clientId, super.toString());  
    }
    
    public ControlMessage cloneDeep (){
    	byte [] resArray = new byte[this.controlMessage.length];
    	System.arraycopy(controlMessage, 0, resArray, 0, controlMessage.length);
    	return new ControlMessage(this.clientId, resArray);
    }
    
    public boolean equalsDeep(ControlMessage other){
		if (!Arrays.equals(other.controlMessage, this.controlMessage)) {
			return false;
		}
    	return (this.clientId == other.clientId);
    }
}
