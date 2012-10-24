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

public class Leader extends PaxosMessage implements Serializable, CloneableDeep<Leader>, EqualsDeep<Leader>, PaxosDescriptor {

    private static final long serialVersionUID = -3781061394615967506L;

    int leader;

    public int getLeader() {
        return leader;
    }

    public void setLeader(int leader) {
        this.leader = leader;
    }

    public Leader() {
    }

    public Leader(int leader) {
        this.leader = leader;
    }


    @Override
    public String toString() {
        return String.format("{Leader is %d %s}", leader, super.toString());
    }

    public Leader cloneDeep() {
        return new Leader(this.leader);
    }

    public boolean equalsDeep(Leader other) {
        return this.leader == other.leader;
    }

    @Override
    public List<PaxosMessage> buildMessages(PaxosState state) {
        return Arrays.<PaxosMessage>asList(this);
    }
}
