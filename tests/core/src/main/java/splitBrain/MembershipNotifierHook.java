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
package splitBrain; 

import org.apache.geode.cache.*;
import org.apache.geode.distributed.DistributedSystem;
import hydra.*;
import hydra.blackboard.*;
import util.*;

import java.util.*;

/** Test hook callbacks to be invoked when a force disconnect occurs.
 */
public class MembershipNotifierHook implements com.gemstone.gemfire.distributed.internal.membership.MembershipTestHook {

  public void beforeMembershipFailure(String reason, Throwable cause) {
     Log.getLogWriter().info("Invoked MembershipNotifierHook, beforeMembershipFailure with reason: " + reason + ", throwable: " + cause);
     ControllerBB.signalMembershipFailureBegun();
  }
  
  public void afterMembershipFailure(String reason, Throwable cause) {
     Log.getLogWriter().info("Invoked MembershipNotifierHook, afterMembershipFailure with reason: " + reason + ", throwable: " + cause);
     ControllerBB.signalMembershipFailureComplete();
  }
  
}
