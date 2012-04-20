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

package com.yahoo.paxos.server;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.handler.execution.ExecutionHandler;
import org.jboss.netty.handler.execution.MemoryAwareThreadPoolExecutor;

import com.yahoo.pasc.PascRuntime;
import com.yahoo.paxos.messages.serialization.ManualDecoder;
import com.yahoo.paxos.state.PaxosState;
import com.yahoo.paxos.statemachine.StateMachine;

/**
 * @author maysam
 * 
 */
public class PipelineFactory implements ChannelPipelineFactory {

    private ChannelHandler channelHandler = null;

    private ExecutionHandler executionHandler;
//    private DownstreamExecutionHandler downstreamExecutionHandler;
    private boolean twoStages;

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
    public PipelineFactory(PascRuntime<PaxosState> runtime, StateMachine stateMachine, ServerConnection serverConnection, int threads, final int id, 
            boolean twoStages) {
        super();
        this.executionHandler = new ExecutionHandler(new MemoryAwareThreadPoolExecutor(1, 1024 * 1024,
                1024 * 1024 * 1024, 10, TimeUnit.SECONDS, new ThreadFactory() {
                    private int count = 0;

                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, id + "-" + count++);
                    }
                }));
//        this.executionHandler = new ExecutionHandler(Executors.newSingleThreadExecutor(new ThreadFactory() {
//                    private int count = 0;
//
//                    @Override
//                    public Thread newThread(Runnable r) {
//                        return new Thread(r, id + "-" + count++);
//                    }
//                }));
//        this.downstreamExecutionHandler = new DownstreamExecutionHandler(Executors.newCachedThreadPool());
        this.channelHandler = new ServerHandler(runtime, stateMachine, serverConnection);
        this.twoStages = twoStages;
    }

    public ChannelPipeline getPipeline() throws Exception {
        ChannelPipeline pipeline = Channels.pipeline();
        // pipeline.addLast("decoder", new KryoDecoder(kryo));
        // pipeline.addLast("encoder", new KryoEncoder(kryo));


        pipeline.addLast("decoder", new ManualDecoder());
//        pipeline.addLast("encoder", new ManualEncoder());
        
        if (twoStages) {
            pipeline.addLast("executor", executionHandler);
        }
//        pipeline.addLast("downstreamExecutor", downstreamExecutionHandler);

        pipeline.addLast("handler", channelHandler);
        return pipeline;
    }

}
