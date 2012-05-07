package com.yahoo.pasc.paxos.state;

import com.yahoo.pasc.CloneableDeep;
import com.yahoo.pasc.EqualsDeep;

public class DigestidDigest implements EqualsDeep<DigestidDigest>, CloneableDeep<DigestidDigest>{
	private int digestId;
	private long digest;

	public DigestidDigest(int digestId, long digest) {
		this.digestId = digestId;
		this.digest = digest;
	}

	public int getDigestId() {
		return digestId;
	}
	public long getDigest() {
		return digest;
	}

    @Override
    public DigestidDigest cloneDeep() {
        return new DigestidDigest(digestId, digest);
    }

    @Override
    public boolean equalsDeep(DigestidDigest other) {
        return this.digestId == other.digestId && this.digest == other.digest;
    }
    
    @Override
    public String toString() {
        return String.format("{ digestId %d digest %d }", digestId, digest);
    }
}
