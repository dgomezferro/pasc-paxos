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

package com.yahoo.pasc.paxos.server;

import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.handler.execution.ExecutionHandler;

import com.yahoo.pasc.paxos.messages.serialization.ManualDecoder;

public class PipelineFactory implements ChannelPipelineFactory {

    private ChannelHandler channelHandler = null;

    private ExecutionHandler executionHandler;
    private boolean twoStages;
    private boolean protection;

    /**
     * Constructor
     * 
     * @param channelGroup
     * @param pipelineExecutor
     * @param answer
     * @param to
     *            The shared timestamp oracle
     * @param shared
     *            The shared state among handlers
     */
    public PipelineFactory(ChannelHandler handler, ExecutionHandler executionHandler, boolean twoStages, boolean protection) {
        super();
        this.executionHandler = executionHandler;
        this.channelHandler = handler;
        this.twoStages = twoStages;
        this.protection = protection;
    }

    public ChannelPipeline getPipeline() throws Exception {
        ChannelPipeline pipeline = Channels.pipeline();
        pipeline.addLast("decoder", new ManualDecoder(protection));
        if (twoStages) {
            pipeline.addLast("executor", executionHandler);
        }
        pipeline.addLast("handler", channelHandler);
        return pipeline;
    }

}
