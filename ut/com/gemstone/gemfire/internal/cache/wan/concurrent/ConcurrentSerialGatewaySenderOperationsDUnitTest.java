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
/**
 * 
 */
package com.gemstone.gemfire.internal.cache.wan.concurrent;

import java.util.Set;

import com.gemstone.gemfire.cache.wan.GatewaySender;
import com.gemstone.gemfire.internal.cache.wan.WANTestBase;
import com.gemstone.gemfire.internal.cache.wan.serial.SerialGatewaySenderImpl;
import com.gemstone.gemfire.cache.util.Gateway.OrderPolicy;
import dunit.AsyncInvocation;

/**
 * @author skumar
 *
 */
public class ConcurrentSerialGatewaySenderOperationsDUnitTest  extends WANTestBase {

  private static final long serialVersionUID = 1L;

  public ConcurrentSerialGatewaySenderOperationsDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
  }

  public void testSerialGatewaySenderOperationsWithoutStarting() {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        false, 100, 10, false, true, null, true, 5, OrderPolicy.KEY });
    vm5.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        false, 100, 10, false, true, null, true, 5, OrderPolicy.KEY });

    vm2.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", null, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", null, isOffHeap() });

    vm4.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "ln", isOffHeap() });
    vm5.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "ln", isOffHeap() });
    vm6.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "ln", isOffHeap() });
    vm7.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "ln", isOffHeap() });

    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_RR",
        1000 });
    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_RR",
        10000 });

    vm4.invoke(ConcurrentSerialGatewaySenderOperationsDUnitTest.class,
        "verifyGatewaySenderOperations", new Object[] { "ln" });
    vm5.invoke(ConcurrentSerialGatewaySenderOperationsDUnitTest.class,
        "verifyGatewaySenderOperations", new Object[] { "ln" });

  }

  
  public void testStartPauseResumeSerialGatewaySender() {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        false, 100, 10, false, true, null, true, 5 , OrderPolicy.KEY});
    vm5.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        false, 100, 10, false, true, null, true, 5 , OrderPolicy.KEY});

    vm2.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", null, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", null, isOffHeap() });

    vm4.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "ln", isOffHeap() });
    vm5.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "ln", isOffHeap() });
    vm6.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "ln", isOffHeap() });
    vm7.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "ln", isOffHeap() });

    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_RR",
        1000 });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_RR",
        10000 });

    vm4.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });

    vm4.invoke(ConcurrentSerialGatewaySenderOperationsDUnitTest.class,
        "verifySenderPausedState", new Object[] { "ln" });
    vm5.invoke(ConcurrentSerialGatewaySenderOperationsDUnitTest.class,
        "verifySenderPausedState", new Object[] { "ln" });

    AsyncInvocation inv1 = vm4.invokeAsync(WANTestBase.class, "doPuts",
        new Object[] { testName + "_RR", 1000 });

    vm4.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });

    vm4.invoke(ConcurrentSerialGatewaySenderOperationsDUnitTest.class,
        "verifySenderResumedState", new Object[] { "ln" });
    vm5.invoke(ConcurrentSerialGatewaySenderOperationsDUnitTest.class,
        "verifySenderResumedState", new Object[] { "ln" });

    try {
      inv1.join();
    } catch (InterruptedException e) {
      e.printStackTrace();
      fail("Interrupted the async invocation.");
    }

    getLogWriter().info("Completed puts in the region");

    vm4.invoke(ConcurrentSerialGatewaySenderOperationsDUnitTest.class,
        "validateQueueContentsForConcurrentSerialGatewaySender", new Object[] { "ln", 0 });
    vm5.invoke(ConcurrentSerialGatewaySenderOperationsDUnitTest.class,
        "validateQueueContentsForConcurrentSerialGatewaySender", new Object[] { "ln", 0 });
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_RR", 10000 });
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_RR", 10000 });

  }

  public void testStopSerialGatewaySender() throws Throwable {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        false, 100, 10, false, true, null, true, 3, OrderPolicy.KEY});
    vm5.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        false, 100, 10, false, true, null, true, 3, OrderPolicy.KEY });

    vm2.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", null, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", null, isOffHeap() });

    vm4.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "ln", isOffHeap() });
    vm5.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "ln", isOffHeap() });
    vm6.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "ln", isOffHeap() });
    vm7.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "ln", isOffHeap() });

    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_RR",
        1000 });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_RR",
        1000 });
    
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_RR", 1000 });
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_RR", 1000 });
    
    vm4.invoke(WANTestBase.class, "stopSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "stopSender", new Object[] { "ln" });

    vm4.invoke(ConcurrentSerialGatewaySenderOperationsDUnitTest.class,
        "verifySenderStoppedState", new Object[] { "ln" });
    vm5.invoke(ConcurrentSerialGatewaySenderOperationsDUnitTest.class,
        "verifySenderStoppedState", new Object[] { "ln" });

    /**
     * Should have no effect on GatewaySenderState
     */
    vm4.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });

    vm4.invoke(ConcurrentSerialGatewaySenderOperationsDUnitTest.class,
        "verifySenderStoppedState", new Object[] { "ln" });
    vm5.invoke(ConcurrentSerialGatewaySenderOperationsDUnitTest.class,
        "verifySenderStoppedState", new Object[] { "ln" });

    AsyncInvocation vm4async = vm4.invokeAsync(WANTestBase.class, "startSender", new Object[] { "ln" });
    AsyncInvocation vm5async = vm5.invokeAsync(WANTestBase.class, "startSender", new Object[] { "ln" });
    int START_WAIT_TIME = 30000;
    vm4async.getResult(START_WAIT_TIME);
    vm5async.getResult(START_WAIT_TIME);

    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_RR",
      10000 });

    vm4.invoke(ConcurrentSerialGatewaySenderOperationsDUnitTest.class,
        "verifySenderResumedState", new Object[] { "ln" });
    vm5.invoke(ConcurrentSerialGatewaySenderOperationsDUnitTest.class,
        "verifySenderResumedState", new Object[] { "ln" });
    
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_RR", 10000 });
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_RR", 10000 });
    
  }

  public void testStopOneSerialGatewaySenderBothPrimary() {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        false, 100, 10, false, true, null, true, 4, OrderPolicy.KEY });
    vm5.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        false, 100, 10, false, true, null, true, 4, OrderPolicy.KEY });

    vm2.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", null, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", null, isOffHeap() });

    vm4.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "ln", isOffHeap() });
    vm5.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "ln", isOffHeap() });
    vm6.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "ln", isOffHeap() });
    vm7.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "ln", isOffHeap() });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_RR",
        1000 });

    vm4.invoke(WANTestBase.class, "stopSender", new Object[] { "ln" });

    vm4.invoke(ConcurrentSerialGatewaySenderOperationsDUnitTest.class,
        "verifySenderStoppedState", new Object[] { "ln" });
    
    vm5.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_RR",
        10000 });
    
    getLogWriter().info("Completed puts in the region");

    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_RR", 10000 });
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_RR", 10000 });
  }

  public void Bug46921_testStopOneSerialGatewaySender_PrimarySecondary() {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        false, 100, 10, false, true, null, true, 4, OrderPolicy.PARTITION });
    vm5.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        false, 100, 10, false, true, null, true, 4, OrderPolicy.PARTITION });

    vm2.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", null, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", null, isOffHeap() });

    vm4.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "ln", isOffHeap() });
    vm5.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "ln", isOffHeap() });
    vm6.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "ln", isOffHeap() });
    vm7.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "ln", isOffHeap() });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_RR",
        1000 });

    vm4.invoke(WANTestBase.class, "stopSender", new Object[] { "ln" });

    vm4.invoke(ConcurrentSerialGatewaySenderOperationsDUnitTest.class,
        "verifySenderStoppedState", new Object[] { "ln" });
    
    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_RR",
        10000 });
    
    getLogWriter().info("Completed puts in the region");

    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_RR", 10000 });
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_RR", 10000 });
  }
  
  public void testStopOneSender_StartAnotherSender() {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm2.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", null, isOffHeap() });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm4.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        false, 100, 10, false, true, null, true, 4, OrderPolicy.PARTITION });
    vm4.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "ln", isOffHeap() });
    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_RR",
        1000 });
    vm4.invoke(WANTestBase.class, "stopSender", new Object[] { "ln" });
    vm4.invoke(ConcurrentSerialGatewaySenderOperationsDUnitTest.class,
        "verifySenderStoppedState", new Object[] { "ln" });

    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
      false, 100, 10, false, true, null, true, 4, OrderPolicy.PARTITION });
    vm5.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
      testName + "_RR", "ln", isOffHeap() });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm5.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_RR",
        10000 });
    getLogWriter().info("Completed puts in the region");
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_RR", 10000 });
  }

  public void test_Bug44153_StopOneSender_StartAnotherSender_CheckQueueSize() {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm4.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        false, 100, 10, false, true, null, true, 3, OrderPolicy.PARTITION });
    vm4.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "ln", isOffHeap() });
    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_RR",
        1000 });
    vm4.invoke(WANTestBase.class, "validateQueueContentsForConcurrentSerialGatewaySender", new Object[] { "ln", 1000});

    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
      false, 100, 10, false, true, null, true, 3, OrderPolicy.PARTITION });
    vm5.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "ln", isOffHeap() });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "validateQueueContentsForConcurrentSerialGatewaySender", new Object[] { "ln", 1000});

    vm4.invoke(WANTestBase.class, "stopSender", new Object[] { "ln" });
    vm4.invoke(ConcurrentSerialGatewaySenderOperationsDUnitTest.class,
        "verifySenderStoppedState", new Object[] { "ln" });
    vm4.invoke(WANTestBase.class, "validateQueueClosedForConcurrentSerialGatewaySender", new Object[] { "ln"});


    vm5.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_RR",
        10000 });

    vm5.invoke(WANTestBase.class, "validateQueueContentsForConcurrentSerialGatewaySender", new Object[] { "ln",
        11000 });
    vm4.invoke(WANTestBase.class, "validateQueueClosedForConcurrentSerialGatewaySender", new Object[] { "ln"});

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm2.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", null, isOffHeap() });
    getLogWriter().info("Completed puts in the region");
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_RR", 10000 });
  }
  
  public static void verifySenderPausedState(String senderId) {
    Set<GatewaySender> senders = cache.getGatewaySenders();
    SerialGatewaySenderImpl sender = null;
    for (GatewaySender s : senders) {
      if (s.getId().equals(senderId)) {
        sender = (SerialGatewaySenderImpl)s;
        break;
      }
    }
    assertTrue(sender.isPaused());
  }

  public static void verifySenderResumedState(String senderId) {
    Set<GatewaySender> senders = cache.getGatewaySenders();
    SerialGatewaySenderImpl sender = null;
    for (GatewaySender s : senders) {
      if (s.getId().equals(senderId)) {
        sender = (SerialGatewaySenderImpl)s;
        break;
      }
    }
    assertFalse(sender.isPaused());
    assertTrue(sender.isRunning());
  }

  public static void verifySenderStoppedState(String senderId) {
    Set<GatewaySender> senders = cache.getGatewaySenders();
    SerialGatewaySenderImpl sender = null;
    for (GatewaySender s : senders) {
      if (s.getId().equals(senderId)) {
        sender = (SerialGatewaySenderImpl)s;
        break;
      }
    }
    assertFalse(sender.isRunning());
    assertFalse(sender.isPaused());
  }

  public static void verifyGatewaySenderOperations(String senderId) {
    Set<GatewaySender> senders = cache.getGatewaySenders();
    GatewaySender sender = null;
    for (GatewaySender s : senders) {
      if (s.getId().equals(senderId)) {
        sender = s;
        break;
      }
    }
    assertFalse(sender.isPaused());
    assertFalse(((SerialGatewaySenderImpl)sender).isRunning());
    sender.pause();
    sender.resume();
    sender.stop();
  }
}
