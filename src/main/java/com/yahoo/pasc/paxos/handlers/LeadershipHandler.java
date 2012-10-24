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

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.pasc.paxos.messages.Leader;
import com.yahoo.pasc.paxos.messages.PaxosDescriptor;
import com.yahoo.pasc.paxos.messages.Prepare;
import com.yahoo.pasc.paxos.state.PaxosState;

public class LeadershipHandler extends PaxosHandler<Leader> {

    private static final Logger LOG = LoggerFactory.getLogger(LeadershipHandler.class);

    @Override
    public List<PaxosDescriptor> processMessage(Leader l, PaxosState state) {
        List<PaxosDescriptor> descriptors = new ArrayList<PaxosDescriptor>();
        LOG.debug("[" + state.getServerId() + "] Current leader " + state.getLeaderId() + " new leader "
                + l.getLeader());
        if (l.getLeader() == state.getLeaderId()) {
            // no change
            return null;
        }

        boolean newLeader = l.getLeader() == state.getServerId();

        if (newLeader) {
            int ballot = generateBallot(state);
            state.setBallotProposer(ballot);
            descriptors.add(new Prepare(state.getServerId(), ballot, state.getMaxExecuted()));
        }

        state.setIsLeader(newLeader);
        state.setLeaderId(l.getLeader());
        state.setCompletedPhaseOne(false);
        descriptors.add(new Leader(l.getLeader()));

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
