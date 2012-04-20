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

package com.yahoo.paxos.handlers;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.pasc.Message;
import com.yahoo.paxos.messages.Digest;
import com.yahoo.paxos.messages.PaxosDescriptor;
import com.yahoo.paxos.state.DigestStore;
import com.yahoo.paxos.state.PaxosState;

public class DigestHandler extends PaxosHandler<Digest> {

    private static final Logger LOG = LoggerFactory.getLogger(DigestHandler.class);

    @Override
    public boolean guardPredicate(Message receivedMessage) {
        return receivedMessage instanceof Digest;
    }

    @Override
    public List<PaxosDescriptor> processMessage(Digest digest, PaxosState state) {
        
//        LOG.trace("Received digest {}.", digest);
        
        storeDigest(digest.getDigestId(), digest.getDigest(), false, state);
        return null;
    }

    public static void storeDigest(long digestId, long digest, boolean mine, PaxosState state) {
        long firstDigestId = state.getFirstDigestId();
        int maxDigests = state.getMaxDigests();
        
//        LOG.trace("Storing digest {} id {} mine {}.", new Object[] {digest, digestId, mine});
        
        if (digestId < firstDigestId) {
//            LOG.trace("Ignoring digest for id {}, first digest: {}.", digestId, firstDigestId);
            // ignore it
            return;
        } else if (firstDigestId + maxDigests <= digestId) {
            LOG.error("We reached a checkpoint but have no space for its digest {}.", digest);
            return;
        }
        
        DigestStore store = state.getDigestStoreElement(digestId);
        int servers = state.getServers();
        if (store == null || store.getDigestId() != digestId) {
//            LOG.trace("New store. (Old one: {})", store);
            store = new DigestStore(digestId, servers);
            state.setDigestStoreElement(digestId, store);
        }
        if (mine) {
            store.addMine(digest);
        } else {
            store.addRemote(digest);
        }

        raiseFirstDigest(state);
    }

    public static void raiseFirstDigest(PaxosState state) {
        long firstInstanceId = state.getFirstInstanceId();
        int checkpointPeriod = state.getCheckpointPeriod();
        int quorum = state.getDigestQuorum();
        long firstDigestId = state.getFirstDigestId();
        DigestStore store = state.getDigestStoreElement(firstDigestId);
        while (store.matches(quorum)) {
//            LOG.trace("Matching digest {} ", firstDigestId);
            firstInstanceId = firstDigestId * checkpointPeriod + 1;
//            LOG.trace("Setting first instance id to {}", firstInstanceId);
            firstDigestId++;

            store = state.getDigestStoreElement(firstDigestId);
            if (store == null || store.getDigestId() < firstDigestId) {
                store = new DigestStore(firstDigestId, state.getServers());
                state.setDigestStoreElement(firstDigestId, store);
            }
        }
        state.setFirstDigestId(firstDigestId);
        state.setFirstInstanceId(firstInstanceId);
    }
}
