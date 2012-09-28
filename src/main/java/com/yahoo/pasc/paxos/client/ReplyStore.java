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

package com.yahoo.pasc.paxos.client;

import java.util.Arrays;
import java.util.BitSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.pasc.CloneableDeep;
import com.yahoo.pasc.EqualsDeep;
import com.yahoo.pasc.paxos.messages.Reply;

public class ReplyStore implements CloneableDeep<ReplyStore>, EqualsDeep<ReplyStore> {

    private static final Logger LOG = LoggerFactory.getLogger(ReplyStore.class);

    private long digestId;
    private byte replies[][];
    private int counts[];
    private int size;
    BitSet senders;

    public ReplyStore(int servers) {
        this.replies = new byte[servers][];
        this.counts = new int[servers];
        this.senders = new BitSet(servers);
        this.size = 0;
    }

    public void addRemote(int senderId, Reply reply) {
        if (!senders.get(senderId)) {
            senders.set(senderId);
            int i = 0;
            for (; i < size; ++i) {
                if (Arrays.equals(replies[i], reply.getValue())) {
                    counts[i]++;
                    return;
                } else {
                    LOG.warn("State divergence adding reply. \n Stored: {} \n Received: {} \n Reply: {}",
                            new Object[] {Arrays.toString(replies[i]), Arrays.toString(reply.getValue()), reply});
                }
            }
            replies[i] = reply.getValue();
            counts[i] = 1;
            size++;
        }
    }

    public int getMatch(int quorum) {
        for (int i = 0; i < size; ++i) {
            if (counts[i] >= quorum) {
                return i;
            }
        }
        return -1;
    }

    public byte[] getStableReply(int quorum) {
        int match = getMatch(quorum);
        if (match == -1) {
            return null;
        } else {
            return replies[match];
        }
    }

    @Override
    public ReplyStore cloneDeep() {
        ReplyStore clone = new ReplyStore(replies.length);
        clone.size = this.size;
        clone.senders = new BitSet();
        clone.senders.or(senders);
        for (int i = 0; i < size; ++i) {
            int len = this.replies[i].length;
            clone.replies[i] = new byte[len];
            System.arraycopy(this.replies[i], 0, clone.replies[i], 0, len);
            clone.counts[i] = this.counts[i];
        }
        return clone;
    }

    @Override
    public boolean equalsDeep(ReplyStore other) {
        if (this.digestId != other.digestId)
            return false;
        if (this.size != other.size)
            return false;
        if (!this.senders.equals(other.senders))
            return false;
        for (int i = 0; i < this.size; ++i) {
            if (!Arrays.equals(this.replies[i], other.replies[i]))
                return false;
            if (this.counts[i] != other.counts[i])
                return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return String.format("{ReplyStore id:%d size:%d replies:%s counts:%s", digestId,
                size, Arrays.toString(replies), Arrays.toString(counts));
    }
}
