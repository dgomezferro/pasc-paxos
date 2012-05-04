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

public enum MessageType {

    REQUEST, INLINEREQ, REPLY, ACCEPT, ACCEPTED, DIGEST, EXECUTE, PREREPLY, HELLO;
    
    public static PaxosMessage getMessage(MessageType m) {
        switch (m) {
        case REQUEST:
            return new Request();
        case INLINEREQ:
            return new InlineRequest();
        case REPLY:
            return new Reply();
        case ACCEPT:
            return new Accept();
        case ACCEPTED:
            return new Accepted();
        case DIGEST:
            return new Digest();
        case EXECUTE:
            return new Execute();
        case PREREPLY:
            return new PreReply();
        case HELLO:
            return new Hello();
        }
        throw new IllegalArgumentException("Unknown message type: " + m);
    }
    
    public static MessageType getMessageType(PaxosMessage m) {
        if (m instanceof InlineRequest)
            return INLINEREQ;
        if (m instanceof Request)
            return REQUEST;
        if (m instanceof Reply)
            return REPLY;
        if (m instanceof Accept)
            return ACCEPT;
        if (m instanceof Accepted)
            return ACCEPTED;
        if (m instanceof Digest)
            return DIGEST;
        if (m instanceof Execute)
            return EXECUTE;
        if (m instanceof PreReply)
            return PREREPLY;
        if (m instanceof Hello)
            return HELLO;
        throw new IllegalArgumentException("Unknown message type: " + m);
    }
}
