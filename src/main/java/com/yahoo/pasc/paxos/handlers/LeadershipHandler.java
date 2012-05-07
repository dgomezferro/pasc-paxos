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

package com.yahoo.pasc.paxos.handlers;

import java.util.Arrays;
import java.util.List;

import com.yahoo.pasc.Message;
import com.yahoo.pasc.paxos.messages.LeadershipChange;
import com.yahoo.pasc.paxos.messages.PaxosDescriptor;
import com.yahoo.pasc.paxos.messages.Prepare;
import com.yahoo.pasc.paxos.state.PaxosState;

public class LeadershipHandler extends PaxosHandler<LeadershipChange> {

//    private static final Logger LOG = LoggerFactory.getLogger(LeadershipHandler.class);

    @Override
    public boolean guardPredicate(Message receivedMessage) {
        return receivedMessage instanceof LeadershipChange;
    }

    @Override
    public List<PaxosDescriptor> processMessage(LeadershipChange lc, PaxosState state) {
        List<PaxosDescriptor> descriptors = null;
        if (lc.isLeader()) {
            int ballot = generateBallot(state);
            state.setBallotProposer(ballot);
            descriptors = Arrays.<PaxosDescriptor>asList(new Prepare(state.getServerId(), ballot, state.getMaxExecuted()));
        }

        state.setIsLeader(lc.isLeader());
        state.setCompletedPhaseOne(false);

        return descriptors;
    }

    private int generateBallot(PaxosState state) {
        int currentBallot = state.getBallotProposer();
        int servers = state.getServers();
        int ballotOrder = currentBallot / servers + 1;
        int serverId = state.getServerId();

        return ballotOrder * servers + serverId;
    }
}
