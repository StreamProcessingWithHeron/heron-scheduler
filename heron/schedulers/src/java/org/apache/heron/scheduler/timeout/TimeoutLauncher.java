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
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.heron.common.basics.SysUtils;
import org.apache.heron.scheduler.local.LocalContext;
import org.apache.heron.scheduler.utils.LauncherUtils;
import org.apache.heron.scheduler.utils.Runtime;
import org.apache.heron.scheduler.utils.SchedulerUtils;
import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.Key;
import org.apache.heron.spi.packing.PackingPlan;
import org.apache.heron.spi.scheduler.ILauncher;
import org.apache.heron.spi.scheduler.IScheduler;
import org.apache.heron.spi.utils.ShellUtils;


public class TimeoutLauncher implements ILauncher { 
  private static final Logger LOG = Logger.getLogger(TimeoutLauncher.class.getName());

  private Config conf;
  private Config runt;
  private String workingDirectory;

  @Override
  public void initialize(Config config, Config runtime) {
    this.conf = Config.toLocalMode(config);
    this.runt = Config.toLocalMode(runtime);
    this.workingDirectory = LocalContext.workingDirectory(conf);
  }

  @Override
  public void close() {} 

  @Override
  public boolean launch(PackingPlan packing) {
    prepareDirectroy(); 
    if (conf.getBooleanValue(Key.SCHEDULER_IS_SERVICE)) { 
      return startSchedulerAsyncProcess();
    } else {
      return onScheduleAsLibrary(packing); 
    }
  }

  private void prepareDirectroy() {
    SchedulerUtils.createOrCleanDirectory(workingDirectory); 
  
    Path heronCore =
      Paths.get(LocalContext.corePackageDirectory(conf)); 
    Path heronCoreLink = 
      Paths.get(workingDirectory, "heron-core"); 
    try {
      Files.createSymbolicLink(heronCoreLink, heronCore); 
    } catch (IOException e) {
      e.printStackTrace();
    }
  
    String topoURI = 
      Runtime.topologyPackageUri(runt).toString(); 
    String topoDest = 
      Paths.get(workingDirectory, "topology.tar.gz")
           .toString(); 
    SchedulerUtils.extractPackage(
      workingDirectory, topoURI, topoDest, true, true); 
  }

  private boolean startSchedulerAsyncProcess() {
    List<Integer> port = IntStream
      .generate(() -> SysUtils.getFreePort())
      .limit(SchedulerUtils.PORTS_REQUIRED_FOR_SCHEDULER).boxed()
      .collect(Collectors.toList()); 
    String[] schedulerCmd =
      SchedulerUtils.schedulerCommand(conf, runt, port); 
    ShellUtils.runASyncProcess(schedulerCmd,
      new File(workingDirectory), null); 
    return true; 
  }

  private boolean onScheduleAsLibrary(PackingPlan packing) {
    try {
      IScheduler scheduler = (IScheduler) ClassLoader
          .getSystemClassLoader()
          .loadClass(conf.getStringValue(Key.SCHEDULER_CLASS))
          .newInstance(); 
      scheduler.initialize(conf, LauncherUtils.getInstance()
          .createConfigWithPackingDetails(runt, packing)); 
      return scheduler.onSchedule(packing); 
    } catch (ClassNotFoundException |
             IllegalAccessException |
             InstantiationException e) {
      e.printStackTrace();
    }
    return false; 
  }
}
