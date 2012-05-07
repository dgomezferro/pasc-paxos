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

import it.unimi.dsi.fastutil.longs.LongArrayList;

import java.util.Arrays;
import java.util.List;

import com.yahoo.pasc.CloneableDeep;
import com.yahoo.pasc.EqualsDeep;
import com.yahoo.pasc.paxos.messages.serialization.ManualEncoder;
import com.yahoo.pasc.paxos.state.DigestidDigest;
import com.yahoo.pasc.paxos.state.InstanceRecord;
import com.yahoo.pasc.paxos.state.PaxosState;

public class Prepared extends PaxosMessage implements CloneableDeep<Prepared>, EqualsDeep<Prepared> {

    private static final long serialVersionUID = -1630688049012844435L;

    public static class Descriptor implements PaxosDescriptor, EqualsDeep<Descriptor> {

        int replyBallot;
        int receiver;
        LongArrayList acceptedIR;
        LongArrayList learnedIR;
        long maxForgotten;
        int quorum;

        public Descriptor(int replyBallot, int receiver, LongArrayList acceptedIR, LongArrayList learnedIR) {
            this.replyBallot = replyBallot;
            this.receiver = receiver;
            this.acceptedIR = acceptedIR;
            this.learnedIR = learnedIR;
            this.maxForgotten = -1;
        }

        public Descriptor(int replyBallot, int receiver, LongArrayList acceptedIR, LongArrayList learnedIR,
                long maxForgotten, int quorum) {
            this.replyBallot = replyBallot;
            this.receiver = receiver;
            this.acceptedIR = acceptedIR;
            this.learnedIR = learnedIR;
            this.maxForgotten = maxForgotten;
            this.quorum = quorum;
        }

        @Override
        public List<PaxosMessage> buildMessages(PaxosState state) {
            InstanceRecord[] acceptedReqs = new InstanceRecord[acceptedIR.size()];
            for (int i = 0; i < acceptedIR.size(); i++) {
                acceptedReqs[i] = state.getInstancesElement(acceptedIR.get(i));
            }
            Accept[] learnedReqs = new Accept[learnedIR.size()];
            for (int i = 0; i < learnedIR.size(); i++) {
                learnedReqs[i] = (Accept) new Accept.Descriptor(learnedIR.get(i)).buildMessages(state).get(0);
                learnedReqs[i].storeReplica(learnedReqs[i]);
            }

            if (maxForgotten > 0) {
                int digestId = (int) (maxForgotten / state.getCheckpointPeriod());
                DigestidDigest digest = new DigestidDigest((int) (maxForgotten / state.getCheckpointPeriod()), state
                        .getDigestStoreElement(digestId).getStableDigest(quorum));
                return Arrays.<PaxosMessage> asList(new Prepared(state.getServerId(), receiver, replyBallot,
                        acceptedReqs.length, acceptedReqs, learnedReqs.length, learnedReqs, digest, maxForgotten));
            } else {
                return Arrays.<PaxosMessage> asList(new Prepared(state.getServerId(), receiver, replyBallot,
                        acceptedReqs.length, acceptedReqs, learnedReqs.length, learnedReqs));
            }
        }

        @Override
        public boolean equalsDeep(Descriptor other) {
            if (this.acceptedIR.size() != other.acceptedIR.size()) {
                return false;
            }
            for (int i = 0; i < this.acceptedIR.size(); i++) {
                if (!this.acceptedIR.get(i).equals(other.acceptedIR.get(i))) {
                    return false;
                }
            }
            if (this.learnedIR.size() != other.learnedIR.size()) {
                return false;
            }
            for (int i = 0; i < this.learnedIR.size(); i++) {
                if (!this.learnedIR.get(i).equals(other.learnedIR.get(i))) {
                    return false;
                }
            }
            return true;
        }
    }

    int senderId;
    int receiver;
    int replyBallot;
    int acceptedSize;
    InstanceRecord[] acceptedReqs;
    int learnedSize;
    Accept[] learnedReqs;
    DigestidDigest checkpointDigest;
    long maxForgotten;

    public Prepared(int senderId, int receiver, int replyBallot, int acceptedSize, InstanceRecord[] acceptedReqs,
            int learnedSize, Accept[] learnedReqs) {
        this.senderId = senderId;
        this.receiver = receiver;
        this.replyBallot = replyBallot;
        this.acceptedSize = acceptedSize;
        this.acceptedReqs = acceptedReqs;
        this.learnedSize = learnedSize;
        this.learnedReqs = learnedReqs;
        this.checkpointDigest = new DigestidDigest(-1, -1);
        this.maxForgotten = -1;
    }

    public Prepared(int senderId, int receiver, int replyBallot, int acceptedSize, InstanceRecord[] acceptedReqs,
            int learnedSize, Accept[] learnedReqs, DigestidDigest digest, long maxForgotten) {
        this.senderId = senderId;
        this.receiver = receiver;
        this.replyBallot = replyBallot;
        this.acceptedSize = acceptedSize;
        this.acceptedReqs = acceptedReqs;
        this.learnedSize = learnedSize;
        this.learnedReqs = learnedReqs;
        this.checkpointDigest = digest;
        this.maxForgotten = maxForgotten;
    }

    public int getSenderId() {
        return senderId;
    }

    public int getReplyBallot() {
        return replyBallot;
    }

    public int getAcceptedSize() {
        return acceptedSize;
    }

    public InstanceRecord[] getAcceptedReqs() {
        return acceptedReqs;
    }

    public int getLearnedSize() {
        return learnedSize;
    }

    public InstanceRecord getAcceptedReq(int pos) {
        return acceptedReqs[pos];
    }

    public Accept[] getLearnedReqs() {
        return learnedReqs;
    }

    public Accept getLearnedReq(int pos) {
        return learnedReqs[pos];
    }

    public DigestidDigest getCheckpointDigest() {
        return checkpointDigest;
    }

    public long getMaxForgotten() {
        return maxForgotten;
    }

    public int getReceiver() {
        return receiver;
    }

    public void setReceiver(int receiver) {
        this.receiver = receiver;
    }

    public int size() {
        // initial size from primitive fields + 12 bytes for DigestidDigest
        int res = 40;
        for (int i = 0; i < acceptedSize; i++) {
            res += acceptedReqs[i].size();
        }
        for (int i = 0; i < learnedSize; i++) {
            res += ManualEncoder.getSize(learnedReqs[i], true);
        }
        return res;
    }

    @Override
    public String toString() {
        return String.format(
                "{Prepared sent from %d to %d with ballot %d accepted %s learned %s digest %s maxForgotten %d}",
                senderId, receiver, replyBallot, Arrays.toString(acceptedReqs), Arrays.toString(learnedReqs),
                checkpointDigest.toString(), maxForgotten);
    }

    @Override
    public boolean equalsDeep(Prepared other) {
        if (this.senderId != other.senderId || this.receiver != other.receiver || this.replyBallot != other.replyBallot
                || this.acceptedSize != other.acceptedSize || this.learnedSize != other.learnedSize
                || this.maxForgotten != other.maxForgotten) {
            return false;
        }
        if (this.acceptedReqs == null) {
            if (other.acceptedReqs != null) {
                return false;
            }
        } else {
            for (int i = 0; i < acceptedSize; ++i) {
                if (!this.acceptedReqs[i].equalsDeep(other.acceptedReqs[i])) {
                    return false;
                }
            }
        }
        if (this.learnedReqs == null) {
            if (other.learnedReqs != null) {
                return false;
            }
        } else {
            for (int i = 0; i < learnedSize; ++i) {
                if (!this.learnedReqs[i].equalsDeep(other.learnedReqs[i])) {
                    return false;
                }
            }
        }
        if (!this.checkpointDigest.equalsDeep(other.checkpointDigest)) {
            return false;
        }
        return true;
    }

    @Override
    public Prepared cloneDeep() {
        InstanceRecord[] accepted = new InstanceRecord[acceptedReqs.length];
        for (int i = 0; i < acceptedReqs.length; ++i) {
            if (acceptedReqs[i] != null) {
                accepted[i] = acceptedReqs[i].cloneDeep();
            }
        }
        Accept[] learned = new Accept[learnedReqs.length];
        for (int i = 0; i < learnedReqs.length; ++i) {
            if (learnedReqs[i] != null) {
                learned[i] = learnedReqs[i].cloneDeep();
            }
        }
        return new Prepared(senderId, receiver, replyBallot, acceptedSize, accepted, learnedSize, learned,
                checkpointDigest.cloneDeep(), maxForgotten);
    }
}
