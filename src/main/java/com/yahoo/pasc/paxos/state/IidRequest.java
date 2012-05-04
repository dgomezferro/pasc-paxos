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

import java.util.Arrays;

import com.yahoo.pasc.CloneableDeep;
import com.yahoo.pasc.EqualsDeep;

public class IidRequest implements CloneableDeep<IidRequest>, EqualsDeep<IidRequest> {
    long iid;
    byte[] request;

    public IidRequest(long iid, byte[] request) {
        super();
        this.iid = iid;
        this.request = request;
    }

    public IidRequest(long iid) {
        super();
        this.iid = iid;
    }

    public IidRequest(byte[] request) {
        super();
        this.iid = -1;
        this.request = request;
    }

    public long getIid() {
        return iid;
    }

    public void setIid(long iid) {
        this.iid = iid;
    }

    public byte[] getRequest() {
        return request;
    }

    public void setRequest(byte[] request) {
        this.request = request;
    }

    @Override
    public boolean equalsDeep(IidRequest other) {
        return this.iid == other.iid && Arrays.equals(this.request, other.request);
    }

    @Override
    public IidRequest cloneDeep() {
        byte[] copyRequest = null;
        if (request != null) {
            copyRequest = Arrays.copyOf(request, request.length);
        }
        return new IidRequest(iid, copyRequest);
    }

}
