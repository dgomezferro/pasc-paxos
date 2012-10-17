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
import com.yahoo.pasc.Message;

public class InvalidMessage extends PaxosMessage implements Serializable, CloneableDeep<InvalidMessage>, EqualsDeep<InvalidMessage> {

    private static final long serialVersionUID = -3781061394615967506L;

    public InvalidMessage() {
    }

    @Override
    public String toString() {
        return String.format("{Invalid message}",super.toString());
    }

    public InvalidMessage cloneDeep() {
        return new InvalidMessage();
    }

    public boolean equalsDeep(InvalidMessage other) {
        return true;
    }

    @Override
    public void storeReplica(Message m) {
    }

    @Override
    protected boolean verify() {
        return true;
    }
}
