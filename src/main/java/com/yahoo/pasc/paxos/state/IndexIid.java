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

package com.yahoo.pasc.paxos.state;

import com.yahoo.pasc.CloneableDeep;
import com.yahoo.pasc.EqualsDeep;

public class IndexIid implements Comparable<IndexIid>, EqualsDeep<IndexIid>, CloneableDeep<IndexIid> {
    int index;
    long iid;

    public IndexIid() {
        // TODO Auto-generated constructor stub
    }

    public IndexIid(int index, long iid) {
        super();
        this.index = index;
        this.iid = iid;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public long getIid() {
        return iid;
    }

    public void setIid(long iid) {
        this.iid = iid;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof IndexIid)) return false;
        IndexIid ct = (IndexIid) obj;
        return this.index == ct.index && this.iid == ct.iid;
    }
    
    @Override
    public int hashCode() {
        return 37 * index + (int)(iid ^ (iid >>> 32));
    }

    @Override
    public int compareTo(IndexIid o) {
        int diff = (int) (this.iid - o.iid);
        if (diff != 0) return diff;
        return this.index - o.index;
    }
    
    public IndexIid cloneDeep(){
    	return new IndexIid(this.index, this.iid);
    }
    
    public boolean equalsDeep (IndexIid other){
        return this.index == other.index && this.iid == other.iid;
    }
    
    @Override
    public String toString() {
        return String.format("<%d,%d>", index, iid);
    }
}
