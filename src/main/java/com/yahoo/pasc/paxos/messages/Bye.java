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

import com.yahoo.pasc.CloneableDeep;
import com.yahoo.pasc.EqualsDeep;

public class Bye extends PaxosMessage implements Serializable, CloneableDeep<Bye>, EqualsDeep<Bye> {

    private static final long serialVersionUID = -3957750297988762927L;

    int clientId;
    int serverId;

    public Bye(int clientId, int serverId) {
        super();
        this.clientId = clientId;
        this.serverId = serverId;
    }

    public Bye() {

    }

    public int getClientId() {
        return clientId;
    }

    public void setClientId(int clientId) {
        this.clientId = clientId;
    }

    public int getServerId() {
        return serverId;
    }

    public void setServerId(int serverId) {
        this.serverId = serverId;
    }

    @Override
    public Bye cloneDeep() {
        return new Bye(clientId, serverId);
    }

    @Override
    public boolean equalsDeep(Bye other) {
        return clientId == other.clientId && serverId == other.serverId;
    }

    @Override
    public String toString() {
        return String.format("{Bye sent from %d to %d %s}", serverId, clientId, super.toString());
    }
}
