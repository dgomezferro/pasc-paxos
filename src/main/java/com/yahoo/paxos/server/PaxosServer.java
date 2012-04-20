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

import java.io.IOException;
import java.net.MalformedURLException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;

import com.yahoo.pasc.PascRuntime;
import com.yahoo.paxos.handlers.DigestHandler;
import com.yahoo.paxos.handlers.acceptor.AcceptorAccept;
import com.yahoo.paxos.handlers.acceptor.AcceptorPrepare;
import com.yahoo.paxos.handlers.learner.Learner;
import com.yahoo.paxos.handlers.learner.LearnerPreReply;
import com.yahoo.paxos.handlers.proposer.ProposerPrepared;
import com.yahoo.paxos.handlers.proposer.ProposerRequest;
import com.yahoo.paxos.messages.Accept;
import com.yahoo.paxos.messages.Accepted;
import com.yahoo.paxos.messages.Digest;
import com.yahoo.paxos.messages.InlineRequest;
import com.yahoo.paxos.messages.PreReply;
import com.yahoo.paxos.messages.Prepare;
import com.yahoo.paxos.messages.Prepared;
import com.yahoo.paxos.messages.Request;
import com.yahoo.paxos.server.tcp.TcpServer;
import com.yahoo.paxos.server.udp.UdpServer;
import com.yahoo.paxos.state.PaxosState;
import com.yahoo.paxos.statemachine.EmptyStateMachine;

public class PaxosServer {

    /**
     * @param args
     * @throws NoSuchFieldException
     * @throws SecurityException
     * @throws IOException 
     * @throws MalformedURLException
     */
    public static void main(String[] args) throws SecurityException, NoSuchFieldException, IOException {

        CommandLineParser parser = new PosixParser();
        Options options;

        {
            Option id           = new Option("i", true, "client id");
            Option port         = new Option("p", true, "port used by server");
            Option buffer       = new Option("b", true, "number of batched messages");
            Option clients      = new Option("c", true, "clients (hostname:port,...)");
            Option servers      = new Option("s", true, "servers (hostname:port,...)");
            Option maxInstances = new Option("m", true, "max number of instances");
            Option anm          = new Option("a", false, "protection against ANM faults");
            Option udp          = new Option("u", false, "use UDP");
            Option leader       = new Option("l", false, "is leader");
            Option cWindow      = new Option("w", true, "congestion window");
            Option threads      = new Option("t", true, "number of threads");
            Option digests      = new Option("d", true, "max digests");
            Option ckPeriod     = new Option("k", true, "checkpointing period");
            Option inlineThresh = new Option("n", true, "threshold for sending requests iNline with accepts ");
            Option twoStages    = new Option("2", false, "2 stages");
            Option digestQuorum = new Option("q", true, "digest quorum");
            
            options = new Options();
            options.addOption(id).addOption(port).addOption(buffer).addOption(clients).addOption(servers)
                    .addOption(threads).addOption(anm).addOption(udp).addOption(maxInstances).addOption(leader)
                    .addOption(cWindow).addOption(digests).addOption(ckPeriod).addOption(inlineThresh)
                    .addOption(twoStages).addOption(digestQuorum);
        }
        
        CommandLine line = null;
        try {
            line = parser.parse(options, args);
            
            String serverAddresses[] = line.hasOption('s') ? line.getOptionValue('s').split(",") : new String[] { "10.78.36.104:20548", "10.78.36.104:20748" };
            String clientAddresses[] = line.hasOption('c') ? line.getOptionValue('c').split(",") : new String[] { "localhost:9000" };
            int serverId             = line.hasOption('i') ? Integer.parseInt(line.getOptionValue('i')) : 0;
            int batchSize            = line.hasOption('b') ? Integer.parseInt(line.getOptionValue('b')) : 1;
            int port                 = line.hasOption('p') ? Integer.parseInt(line.getOptionValue('p')) : 20548;
            int maxInstances         = line.hasOption('m') ? Integer.parseInt(line.getOptionValue('m')) : 16*1024;
            int congestionWindow     = line.hasOption('w') ? Integer.parseInt(line.getOptionValue('w')) : 1;
            int digests              = line.hasOption('d') ? Integer.parseInt(line.getOptionValue('d')) : 16;
            int inlineThreshold      = line.hasOption('n') ? Integer.parseInt(line.getOptionValue('n')) : 10;
            boolean protection       = line.hasOption('a');
            boolean udp              = line.hasOption('u');
            boolean leader           = line.hasOption('l');
            boolean twoStages        = line.hasOption('2');
            int quorum               = serverAddresses.length / 2 + 1;
            int digestQuorum         = line.hasOption('q') ? Integer.parseInt(line.getOptionValue('q')) : quorum;
            int threads              = line.hasOption('t') ? Integer.parseInt(line.getOptionValue('t')) :
                Runtime.getRuntime().availableProcessors() * 2 + 1;
            
            if (batchSize <= 0) {
                throw new RuntimeException("BatchSize must be greater than 0");
            }
            
            PaxosState state = new PaxosState(maxInstances, batchSize, serverId, leader, quorum, digestQuorum,
                    serverAddresses.length, congestionWindow, digests);
            if (line.hasOption('k')) state.setCheckpointPeriod(Integer.parseInt(line.getOptionValue('k')));
            state.setRequestThreshold(inlineThreshold);

            if (!protection) {
                System.out.println("PANM disabled!");
            }
            
            final PascRuntime<PaxosState> runtime = new PascRuntime<PaxosState>(protection);
            runtime.setState(state);
            runtime.addHandler(Accept.class, new AcceptorAccept());
            runtime.addHandler(Prepare.class, new AcceptorPrepare());
            runtime.addHandler(Accepted.class, new Learner());
            runtime.addHandler(Prepared.class, new ProposerPrepared());
            runtime.addHandler(Request.class, new ProposerRequest());
            runtime.addHandler(InlineRequest.class, new ProposerRequest());
            runtime.addHandler(Digest.class, new DigestHandler());
            runtime.addHandler(PreReply.class, new LearnerPreReply());

            if (udp) {
                new UdpServer(runtime, serverAddresses, clientAddresses, port, threads, serverId).run();
            } else {
                new TcpServer(runtime, new EmptyStateMachine(), serverAddresses, clientAddresses, port, threads, serverId, twoStages).run();
            }
        } catch (Exception e) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("Paxos", options);
            
            System.err.println("Unexpected exception: " + e);
            e.printStackTrace();
            
            System.exit(-1);
        }
    }

}
