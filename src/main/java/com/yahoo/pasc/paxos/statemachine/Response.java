package com.yahoo.pasc.paxos.statemachine;

import java.util.ArrayList;
import java.util.List;

import com.yahoo.pasc.paxos.messages.AsyncMessage;

public class Response {
    private byte[] response;
    private List<AsyncMessage> asyncMessages;

    public Response() {
        asyncMessages = new ArrayList<AsyncMessage>();
    }

    public Response(byte[] response, List<AsyncMessage> asyncMessages) {
        super();
        this.response = response;
        this.asyncMessages = asyncMessages;
    }

    public byte[] getResponse() {
        return response;
    }

    public void setResponse(byte[] response) {
        this.response = response;
    }

    public List<AsyncMessage> getAsyncMessages() {
        return asyncMessages;
    }

    public void setAsyncMessages(List<AsyncMessage> asyncMessages) {
        this.asyncMessages = asyncMessages;
    }

}
