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
package hydratest.security;

import org.apache.geode.LogWriter;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.security.AuthenticationFailedException;
import org.apache.geode.security.Authenticator;

import java.security.Principal;
import java.util.Properties;

public class PeerAuthenticator implements Authenticator {

  public static Authenticator create() {
    return new PeerAuthenticator();
  }

  public void init(Properties systemProps, LogWriter systemLogger,
      LogWriter securityLogger) {
  }

  public Principal authenticate(Properties props, DistributedMember member)
      throws AuthenticationFailedException {
    return new IdPrincipal();
  }

  public void close() {
  }
}
