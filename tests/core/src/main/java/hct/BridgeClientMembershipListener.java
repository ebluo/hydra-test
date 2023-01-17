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

package hct;

import hydra.*;
import hydra.blackboard.*;

import org.apache.geode.cache.util.*;

public class BridgeClientMembershipListener extends AbstractListener implements BridgeMembershipListener {

  /** Implementation of BridgeMembershipListener, memberJoined interface
   *
   *  @param BridgeMembershipEvent memberJoined event
   */
  public void memberJoined(BridgeMembershipEvent event) {

    logCall("memberJoined", event);

    // updateCounters
    SharedCounters sc = BBoard.getInstance().getSharedCounters();
    long count = sc.incrementAndRead( BBoard.actualClientJoinedEvents );
    Log.getLogWriter().info("After incrementing counter, actualClientJoinedEvents =  " + count);
  }

  /** Implementation of BridgeMembershipListener, memberLeft interface
   *
   *  @param BridgeMembershipEvent memberLeft event
   */
  public void memberLeft(BridgeMembershipEvent event) {
 
    logCall("memberLeft", event);

    // updateCounters
    SharedCounters sc = BBoard.getInstance().getSharedCounters();
    long count = sc.incrementAndRead( BBoard.actualClientDepartedEvents);
    Log.getLogWriter().info("After incrementing counter, actualClientDepartedEvents =  " + count);
  }

  /** Implementation of BridgeMembershipListener, memberCrashed interface
   *
   *  @param BridgeMembershipEvent memberCrashed event
   */
  public void memberCrashed(BridgeMembershipEvent event) {
    logCall("memberCrashed", event);

    // updateCounters
    SharedCounters sc = BBoard.getInstance().getSharedCounters();
    long count = sc.incrementAndRead( BBoard.actualClientCrashedEvents );
    Log.getLogWriter().info("After incrementing counter, actualClientCrashedEvents =  " + count);
  }
}
