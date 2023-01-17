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
package newWan.serial.filters;

import hydra.Log;
import hydra.ProcessMgr;
import hydra.RemoteTestModule;

import java.io.Serializable;

import newWan.WANBlackboard;

import org.apache.geode.cache.Operation;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.asyncqueue.AsyncEvent;
import org.apache.geode.cache.wan.GatewayEventFilter;
import org.apache.geode.cache.wan.GatewayQueueEvent;
import org.apache.geode.internal.cache.wan.GatewaySenderEventCallbackArgument;
import org.apache.geode.internal.cache.wan.GatewaySenderEventImpl;

public class MyEventFilterBeforeEnqueue implements GatewayEventFilter {

  private String id = new String("MyEventFilterBeforeEnqueue");

  public void afterAcknowledgement(GatewayQueueEvent event) {
    //logCall("afterAcknowledgement", event);
  }

  public boolean beforeEnqueue(GatewayQueueEvent event) {
    //logCall("beforeEnqueue", event);

    // filter event
    if (((String)event.getKey()).contains(WanFilterTest.FILTER_KEY_PRIFIX)) {
      Log.getLogWriter().info(
          "Filtering event beforeEnqueue for key " + event.getKey()
              + " in region " + event.getRegion().getFullPath());
      return false;
    }

    return true;
  }

  public boolean beforeTransmit(GatewayQueueEvent event) {
    //logCall("beforeTransmit", event);

    // filter event
    if (((String)event.getKey()).contains(WanFilterTest.FILTER_KEY_PRIFIX)) {
      WANBlackboard.throwException("Unexpected event for key " + event.getKey() 
          + " in beforeTransmit of filter " + this.getClass().getName() + ". The event should have been already filtered in beforeEnqueue()");      
      return false;
    }
    return true;
  }

  public void close() {

  }

  /**
   * Log that an event occurred.
   * 
   * @param methodName
   *          The name of the CacheEvent method that was invoked.
   * @param event
   *          The event object that was passed to the event.
   */
  public void logCall(String methodName, AsyncEvent event) {
    StringBuffer aStr = new StringBuffer();
    String clientName = RemoteTestModule.getMyClientName();
    aStr.append("Invoked " + this.getClass().getName() + " for key "
        + event.getKey() + ": " + methodName + " in " + clientName + " event="
        + event + "\n");

    aStr.append("   whereIWasRegistered: " + ProcessMgr.getProcessId() + "\n");
    aStr.append("   key: " + event.getKey() + "\n");

    GatewaySenderEventImpl e = (GatewaySenderEventImpl)event;
    aStr.append("   event.getEventId(): " + e.getEventId() + "\n");
    aStr.append("   event.getValue(): " + e.getValueAsString(true) + "\n");

    Region region = event.getRegion();
    aStr.append("   region: " + event.getRegion().getFullPath() + "\n");

    if (region.getAttributes() instanceof PartitionAttributes) {
      aStr.append("   event.getBucketId(): " + e.getBucketId() + "\n");
      aStr.append("   event.getShadowKey(): " + e.getShadowKey() + "\n");
    }

    aStr.append("   callbackArgument: " + e.getSenderCallbackArgument() + "\n");
    if (e.getSenderCallbackArgument() instanceof GatewaySenderEventCallbackArgument) {
      GatewaySenderEventCallbackArgument callback = (GatewaySenderEventCallbackArgument)e
          .getSenderCallbackArgument();
      aStr.append("   callback.getOriginatingDSId(): "
          + callback.getOriginatingDSId() + "\n");
      aStr.append("   callback.getRecipientDSIds(): "
          + callback.getRecipientDSIds() + "\n");
    }

    Operation op = event.getOperation();
    aStr.append("   operation: " + op.toString() + "\n");
    aStr.append("   Operation.isDistributed(): " + op.isDistributed() + "\n");

    Log.getLogWriter().info(aStr.toString());
  }

//  // rahul <todo>: remove equals() method after fixing #44406
//  @Override
//  public boolean equals(Object obj) {
//    return true;
//  }

  @Override
  public String toString() {
    return id;
  }
}
