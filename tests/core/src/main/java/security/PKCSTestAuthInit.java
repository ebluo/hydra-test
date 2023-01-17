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
package security;

import java.util.Properties;

import newWan.security.WanSecurity;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.security.AuthInitialize;
import org.apache.geode.security.AuthenticationFailedException;

import templates.security.PKCSAuthInit;

import hydratest.security.SecurityTestPrms;

/**
 * @author akarayil
 */
public class PKCSTestAuthInit extends PKCSAuthInit {

  public static AuthInitialize create() {
    return new PKCSTestAuthInit();
  }

  public PKCSTestAuthInit() {
    super();
  }

  @Override
  public Properties getCredentials(Properties props, DistributedMember server,
      boolean isPeer) throws AuthenticationFailedException {

    Properties newprops = super.getCredentials(props, server, isPeer);
    if (SecurityTestPrms.useBogusPassword() && !isPeer) {
      newprops.put(PKCSAuthInit.KEYSTORE_ALIAS, "bogus");
    }
    else if (WanSecurity.isInvalid){  // 44650 - for newWan, the task thread and the connection thread are different, hence added a new check.
      newprops.put(PKCSAuthInit.KEYSTORE_ALIAS, "bogus");
    }
    return newprops;
  }
}
