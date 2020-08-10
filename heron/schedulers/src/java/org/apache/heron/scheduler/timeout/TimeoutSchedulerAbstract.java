/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.heron.scheduler.timeout;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.heron.common.basics.SysUtils;
import org.apache.heron.proto.scheduler.Scheduler;
import org.apache.heron.scheduler.local.LocalContext;
import org.apache.heron.scheduler.utils.SchedulerUtils;
import org.apache.heron.scheduler.utils.SchedulerUtils.ExecutorPort;
import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.Context;
import org.apache.heron.spi.scheduler.IScheduler;

public abstract class TimeoutSchedulerAbstract implements IScheduler {
  protected Config conf;
  protected Config runt;
  protected File workingDirectory;

  @Override
  public void initialize(Config config, Config runtime) {
    conf = Config.toClusterMode(config);
    runt = Config.toClusterMode(runtime);
    workingDirectory = new File(LocalContext.workingDirectory(conf));
  }

  @Override
  public void close() {

  }

  @Override
  public List<String> getJobLinks() {
    return null;
  }

  @Override
  public boolean onRestart(Scheduler.RestartTopologyRequest request) {
    return false;
  }

  @Override
  public boolean onUpdate(Scheduler.UpdateTopologyRequest request) {
    return false;
  }

  protected String[] getExecutorCommand(int shardId) {
    Map<ExecutorPort, String> port = 
      ExecutorPort.getRequiredPorts().stream().collect(
        Collectors.toMap(ep -> ep,
          ep -> Integer.toString(SysUtils.getFreePort()))); 
  
    List<String> executorCmd = new ArrayList<>();
    executorCmd.add("timeout"); 
    executorCmd.add(conf.getStringValue(
      "heron.scheduler.timeout.duration", "120")); 
    executorCmd.add(Context.executorBinary(conf)); 
    executorCmd.addAll(
      Arrays.asList(SchedulerUtils.executorCommandArgs(
        conf, runt, port, Integer.toString(shardId)))); 
    return executorCmd.toArray(new String[0]);
  }
}
