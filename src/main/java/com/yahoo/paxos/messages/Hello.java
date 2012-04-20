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

import com.yahoo.pasc.CloneableDeep;
import com.yahoo.pasc.EqualsDeep;

public class Hello extends PaxosMessage implements Serializable, CloneableDeep<Hello>, EqualsDeep<Hello> {

    private static final long serialVersionUID = -3781061394615967506L;

    int clientId;

    public Hello() {
    }

    public Hello(int clientId) {
        this.clientId = clientId;
    }

    public int getClientId() {
        return clientId;
    }

    public void setClientId(int clientId) {
        this.clientId = clientId;
    }

    @Override
    public String toString() {
        return String.format("{Hello sent from %d %s}", clientId, super.toString());
    }

    public Hello cloneDeep() {
        return new Hello(this.clientId);
    }

    public boolean equalsDeep(Hello other) {
        return this.clientId == other.clientId;
    }
}
