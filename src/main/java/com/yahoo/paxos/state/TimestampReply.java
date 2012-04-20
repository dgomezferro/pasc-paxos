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

package com.yahoo.paxos.state;

import com.yahoo.pasc.CloneableDeep;
import com.yahoo.pasc.EqualsDeep;

public class TimestampReply implements EqualsDeep<TimestampReply>, CloneableDeep<TimestampReply> {
	long timestamp;
	byte[] repl;
	
	public TimestampReply(long timestamp, byte[] repl){
		this.timestamp = timestamp;
		this.repl = repl;
	}
	
	public long getTimestamp(){
		return timestamp;
	}
	
	public byte[] getReply(){
		return repl;
	}
	
	public TimestampReply cloneDeep(){
		byte [] otherReply = new byte[repl.length];
		for (int i = 0; i < repl.length; i++){
			otherReply[i] = repl[i];
		}
		return new TimestampReply(this.timestamp, otherReply);
	}
	
	public boolean equalsDeep(TimestampReply other){
		for (int i = 0; i < repl.length; i++){
			byte [] otherReply = other.getReply();
			if (otherReply[i] != repl[i]){
				return false;
			}
		}
		return (this.timestamp == other.timestamp);
	}
	
}
