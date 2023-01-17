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
package cq.ha;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import hydra.*;
import hydra.blackboard.*;
import util.TestException;
import util.TestHelper;
import cq.CQUtilBB;
import hct.ha.*;
import hct.ha.Feeder;
import durableClients.*;

import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.distributed.DistributedSystem;


/**
 * This class is a <code>CacheListener</code> implementation attached to the
 * cache-clients for test validations. The callback methods validate the order
 * of the data coming via the events. This listener is used for no-failover test
 * cases.  
 *
 * In this cq version, we additionally track the expected number of CQEvents:
 * - afterCreate (all should contribute to expected CQ Create Events)
 * - afterInvalidate (all should contribute to expected CQ Destroy Events)
 * - afterUpdate (those with oldValue=null => expected CQ Create Events, others CQ Update Events)
 * - afterDestroy (if previously invalidated, no-op.  Otherwise, increment expected CQ Destroy Event count)
 * 
 * @author Dinesh Patel
 * @author Mitul Bid
 * @author Lynn Hughes-Godfrey
 * 
 */
public class HANoFailoverClientListener extends CacheListenerAdapter
{
  /**
   * A local map to store the last received values for the keys in the callback
   * events.
   */
  private final Map latestValues = new HashMap();

  /** 
   * Counters for entry events received by this VM
   */
  public static long expectedCQCreates = 0;
  public static long expectedCQDestroys = 0;
  public static long expectedCQUpdates = 0;

  /**
   * This method performs the following tasks:<br>
   * 1)Increment the count of afterCreate in Validator<br>
   * 2)Verify that value received in the event is not null as Feeder always does
   * create() on a key with a Long value. <br>
   * 3)If the value is not zero( meaning the first create() call on this key),
   * validate that the new value is exactly one more than the previous value.<br>
   * 4)Update the latest value map for the event key with the new value
   * received.
   * 
   * @param event -
   *          the entry event received in callback
   */
  public void afterCreate(EntryEvent event)
  {
    Validator.createCount++;

    // Set flag once last_key received (so we know all updates have been delivered) 
    String key = (String)event.getKey();
    if (key.equals(hct.ha.Feeder.LAST_KEY)) {
      HAClientQueue.lastKeyReceived = true;
      Log.getLogWriter().info("'last_key' received at client");
    }

    if (key.equals(durableClients.Feeder.LAST_KEY)) {
      DurableClientsTest.receivedLastKey = true;
      Log.getLogWriter().info("'last_key' received at client");
    }

    Long value = (Long)event.getNewValue();

    if (value == null) {
      throwException("value in afterCreate cannot be null: key = " + key);
    }

    if (value.longValue() != 0) {
      validateIncrementByOne(key, value);
      expectedCQCreates++;
    }
    latestValues.put(key, value);
  }

  /**
   * This method performs the following tasks:<br>
   * 1)Increment the count of afterUpdate in Validator<br>
   * 2)Verify that value received in the event is not null as Feeder always
   * generates update on a key with a Long value one more than the previous one
   * on this key.<br>
   * 3)If the oldValue received in the event is null,it implies that Feeder
   * invalidated this key in previous operation. In this case, validate that the
   * newValue received in the event is one more than that stored in the local
   * <code>latestValues</code> map<br>
   * 4)If the oldValue received in the event is not null, validate that the
   * newValue received in the event is one more than the oldValue received in
   * the event.<br>
   * 4)Update the latest value map for the event key with the new value
   * received.
   * 
   * @param event -
   *          the entry event received in callback
   */
  public void afterUpdate(EntryEvent event)
  {
    Validator.updateCount++;
    String key = (String)event.getKey();
    Long newValue = (Long)event.getNewValue();
    Long oldValue = (Long)event.getOldValue();

    if (newValue == null) {
      throwException("newValue in afterUpdate cannot be null: key = " + key);
    }

    if (oldValue == null) {
      validateIncrementByOne(key, newValue);
      expectedCQCreates++;
    }
    else {
      long diff = newValue.longValue() - oldValue.longValue();
      if (diff != 1) {
        throwException("difference expected in newValue and oldValue is 1, but was not for key = "
            + key + " & newVal = " + newValue + " oldValue = " + oldValue);
      }
      expectedCQUpdates++;
    }
    latestValues.put(key, newValue);

  }

  /**
   * This method performs the following tasks:<br>
   * 1)Increment the count of afterInvalidate in Validator<br>
   * 2)Verify that oldValue received in the event is not null as Feeder does
   * invalidate() on only those keys which have non-null values.<br>
   * 3)Update the latest value map for the event key with the oldValue received.
   * 
   * @param event -
   *          the entry event received in callback
   */
  public void afterInvalidate(EntryEvent event)
  {
    Validator.invalidateCount++;
    String key = (String)event.getKey();
    Long oldValue = (Long)event.getOldValue();

    if (oldValue == null) {
      throwException("oldValue in afterInvalidate cannot be null : key = "
          + key);
    }
    expectedCQDestroys++;

    latestValues.put(key, oldValue);
  }

  /**
   * This method performs the following tasks:<br>
   * 1)Increment the count of afterDestroy in Validator<br>
   * 2)If the oldValue in the event is not null, update the latest value map for
   * the event key with the oldValue received. If the value is null, it implies
   * that Feeder did a invalidate() in its previous operation on this key and
   * hence no need to update the local <code>latestValues</code> map.
   * 
   * @param event -
   *          the entry event received in callback
   */
  public void afterDestroy(EntryEvent event)
  {
    Validator.destroyCount++;
    String key = (String)event.getKey();
    Long value = (Long)event.getOldValue();

    if (value != null) {
      latestValues.put(key, value);
      expectedCQDestroys++;
    } 
  }

  /**
   * This method verifies that the given <code>newValue</code>for the key is
   * exactly one more than that in the <code>latestValues</code> map. If the
   * oldValue in <code>latestValues</code> map is null or the above validation
   * fails, {@link #throwException(String)} is called to update the blackboard.
   * 
   * @param key -
   *          key of the callback event
   * @param newValue -
   *          key of the callback event
   */
  private void validateIncrementByOne(String key, Long newValue)
  {
    Long oldValue = (Long)latestValues.get(key);
    if (oldValue == null) {
      throwException("oldValue in latestValues cannot be null: key = " + key
          + " & newVal = " + newValue);
    }
    long diff = newValue.longValue() - oldValue.longValue();
    if (diff != 1) {
      throwException("difference expected in newValue and oldValue is 1, but is was "
          + diff + " for key = " + key + " & newVal = " + newValue);
    }
  }

  /**
   * This method increments the number of exceptions occured counter in the
   * blackboard and put the reason string against the exception number in the
   * shared map.
   * 
   * @param reason -
   *          string description of the cause of the exception.
   */
  public static void throwException(String reason)
  {
		ArrayList reasonArray = null;
		DistributedSystem ds = DistributedSystemHelper.getDistributedSystem();
		String clientName = "CLIENT_"+ds.getName();
		SharedMap shMap = HAClientQueueBB.getBB().getSharedMap();
		if(!shMap.containsKey(clientName)){
			reasonArray = new ArrayList();
		}else{
			reasonArray = (ArrayList) shMap.get(clientName);
		}
		reasonArray.add(reason);
		shMap.put(clientName, reasonArray);
		HAClientQueueBB.getBB().getSharedCounters().increment(HAClientQueueBB.NUM_EXCEPTION);
		Log.getLogWriter().info("Exception : " + TestHelper.getStackTrace(new TestException(reason)));
	  
  }

  /**
   * Method to compare the expected number of CQEvents (determined by region events received
   * by this Listener) with the actual CQEvents received.
   */
  public static void verifyCQEventsReceived() {
    SharedCounters sc = CQUtilBB.getBB().getSharedCounters();
    long actualCQCreates = sc.read(CQUtilBB.NUM_CREATE);
    long actualCQDestroys = sc.read(CQUtilBB.NUM_DESTROY);
    long actualCQUpdates = sc.read(CQUtilBB.NUM_UPDATE);
 
    // Check if all the events are received.
    while (true){
      boolean getNewNumbers = false;
     
      try {
        Thread.sleep(2 * 1000);
      } catch (Exception ex){}

      if (sc.read(CQUtilBB.NUM_CREATE) != actualCQCreates ||
          sc.read(CQUtilBB.NUM_DESTROY) != actualCQDestroys ||
          sc.read(CQUtilBB.NUM_UPDATE) != actualCQUpdates){
        getNewNumbers = true;
      }

      if (getNewNumbers){
        // The cqListeners are still receiving the events.
        actualCQCreates = sc.read(CQUtilBB.NUM_CREATE);
        actualCQDestroys = sc.read(CQUtilBB.NUM_DESTROY);
        actualCQUpdates = sc.read(CQUtilBB.NUM_UPDATE);
        Log.getLogWriter().info("Still receiving the events, will wait to finish...");

        continue;
      }
      
      break;
    }

    StringBuffer aStr = new StringBuffer();
    aStr.append("CQCreate Events expected: " + expectedCQCreates + ", received " + actualCQCreates + "\n");
    aStr.append("CQDestroy Events expected: " + expectedCQDestroys + ", received " + actualCQDestroys + "\n");
    aStr.append("CQUpdate Events expected: " + expectedCQUpdates + ", received " + actualCQUpdates + "\n");
    Log.getLogWriter().info("CQEvent counts : \n" + aStr.toString());

    aStr = new StringBuffer();
    if (expectedCQCreates != actualCQCreates) {
       aStr.append("Expected " + expectedCQCreates + " CQCreate Events but processed " + actualCQCreates + "\n");
    }
    if (expectedCQDestroys != actualCQDestroys) {
       aStr.append("Expected " + expectedCQDestroys + " CQDestroy Events but processed " + actualCQDestroys + "\n");
    }
    if (expectedCQUpdates != actualCQUpdates) {
       aStr.append("Expected " + expectedCQUpdates + " CQUpdate Events but processed " + actualCQUpdates + "\n");
    }
    
    if (aStr.length() > 0) {
       String errMsg = "verifyCQEventsReceived validation failure: \n" + aStr.toString();
       Log.getLogWriter().info(errMsg);
       throw new TestException( errMsg + TestHelper.getStackTrace() );
    } else {
       Log.getLogWriter().info("CQEvent counts successfully verified");
    }
  }
}
