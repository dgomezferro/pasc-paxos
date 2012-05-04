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

package com.yahoo.pasc.paxos.client.messages;

import com.yahoo.pasc.Message;
import com.yahoo.pasc.MessageDescriptor;

public class Submit extends Message {

    public static class Descriptor implements MessageDescriptor {

        private byte[] value;

        public Descriptor(byte[] value) {
            this.value = value;
        }

        public byte[] getValue() {
            return value;
        }

        public void setValue(byte[] value) {
            this.value = value;
        }
    }

    byte[] request;

    public Submit() {
    }

    public Submit(byte[] request) {
        this.request = request;
    }

    public byte[] getRequest() {
        return request;
    }

    public void setRequest(byte[] request) {
        this.request = request;
    }

    @Override
    protected boolean verify() {
        return true;
    }

    @Override
    public void storeReplica(Message m) {

    }

}
