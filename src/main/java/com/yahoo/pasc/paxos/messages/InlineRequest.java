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

import com.yahoo.pasc.CloneableDeep;
import com.yahoo.pasc.EqualsDeep;

public class InlineRequest extends Request implements CloneableDeep<Request>, EqualsDeep<Request> {

    private static final long serialVersionUID = 991039430350128168L;
    
    public InlineRequest() {
        super();
    }

    public InlineRequest(int clientId, long timestamp, byte[] request) {
        super(clientId, timestamp, request);
    }

    @Override
    public String toString() {
        return super.toString().replaceFirst("Request", "InlineRequest");
    }

    @Override
    public Request cloneDeep (){
        byte [] resArray = new byte[this.request.length];
        System.arraycopy(request, 0, resArray, 0, request.length);
        return new InlineRequest(this.clientId, this.timestamp, resArray);
    }
}
