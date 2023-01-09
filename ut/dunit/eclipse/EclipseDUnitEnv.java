/*
 * Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package dunit.eclipse;

import hydra.GemFireDescription;

import java.util.Properties;

import dunit.DUnitEnv;

public class EclipseDUnitEnv extends DUnitEnv {

  @Override
  public String getLocatorString() {
    return DUnitLauncher.getLocatorString();
  }

  @Override
  public String getLocatorAddress() {
    return "localhost";
  }
  
  @Override
  public int getLocatorPort() {
    return DUnitLauncher.locatorPort;
  }

  @Override
  public Properties getDistributedSystemProperties() {
    return DUnitLauncher.getDistributedSystemProperties();
  }

  @Override
  public GemFireDescription getGemfireDescription() {
    return new FakeDescription();
  }

  @Override
  public int getPid() {
    return Integer.getInteger(DUnitLauncher.VM_NUM_PARAM, -1).intValue();
  }

}
