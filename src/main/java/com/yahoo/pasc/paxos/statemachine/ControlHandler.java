package com.yahoo.pasc.paxos.statemachine;

import com.yahoo.pasc.paxos.messages.ControlMessage;

public interface ControlHandler {
    public void handleControlMessage(ControlMessage controlMessage);
}
