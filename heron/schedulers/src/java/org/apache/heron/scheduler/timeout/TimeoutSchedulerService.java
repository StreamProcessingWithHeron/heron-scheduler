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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.heron.proto.scheduler.Scheduler;
import org.apache.heron.scheduler.utils.Runtime;
import org.apache.heron.spi.common.Context;
import org.apache.heron.spi.packing.PackingPlan;
import org.apache.heron.spi.utils.ShellUtils;


public class TimeoutSchedulerService 
    extends TimeoutSchedulerAbstract {
  private Set<Process> processes = new HashSet<>();
  private ExecutorService monitorService =
    Executors.newCachedThreadPool();

  @Override
  public boolean onSchedule(PackingPlan packing) {
    processes.add(ShellUtils.runASyncProcess(
        getExecutorCommand(0), workingDirectory, null)); 
    packing.getContainers().forEach(cp -> processes.add(ShellUtils
        .runASyncProcess(getExecutorCommand(cp.getId()),
            workingDirectory, null))); 
    monitorService.submit(() -> {
      processes.stream().forEach(p -> {
        try {
          p.waitFor(); 
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      });
      monitorService.shutdownNow(); 
      String topologyName = Context.topologyName(runt);
      Runtime.schedulerStateManagerAdaptor(runt)
          .deleteExecutionState(topologyName);
      Runtime.schedulerStateManagerAdaptor(runt)
          .deleteTopology(topologyName);
      Runtime.schedulerStateManagerAdaptor(runt)
          .deleteSchedulerLocation(topologyName); 
      Runtime.schedulerShutdown(runt).terminate(); 
    });
    return true;
  }

  @Override
  public boolean onKill(Scheduler.KillTopologyRequest request) {
    processes.stream().forEach(p -> p.destroy());
    return true;
  }
}

