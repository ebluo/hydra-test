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

import util.*;
import hydra.*;
import org.apache.geode.cache.*;

/** Event Test Listener. 
 *  Does validation of callback objects and that the event is invoked in the
 *  VM where it was created.
 *
 * @see BridgeNotifyBB#isSerialExecution
 * @see EventCountersBB
 *
 * @author Lynn Gallinat
 * @since 5.1
 */
public class TxClientEventListener extends util.AbstractListener implements CacheListener, Declarable {

private boolean isCarefulValidation;

/** noArg constructor which sets isCarefulValidation based on the tests
 *  setting of serialExecution
 */
public TxClientEventListener() {
   TestConfig config = TestConfig.getInstance();
   ConfigHashtable tab = config.getParameters();
   this.isCarefulValidation = tab.booleanAt(Prms.serialExecution);
}

/** Create a new listener and specify whether the test is doing careful validation.
 *
 *  @param isCarefulValidation true if the test is doing careful validate (serial test)
 *         false otherwise.
 */
public TxClientEventListener(boolean isCarefulValidation) {
   this.isCarefulValidation = isCarefulValidation;
}

//==============================================================================
// implementation CacheListener methods
public void afterCreate(EntryEvent event) {
   logCall("afterCreate", event);

   String callbackObj = (String)event.getCallbackArgument();
   if ((callbackObj == null) || ((callbackObj != null) && (!callbackObj.startsWith("ignore")))) {
      incrementAfterCreateCounters(event, EventCountersBB.getBB());
      checkVM();
   }
}

public void afterDestroy(EntryEvent event) {
   logCall("afterDestroy", event);

   String callbackObj = (String)event.getCallbackArgument();
   if ((callbackObj == null) || ((callbackObj != null) && (!callbackObj.startsWith("ignore")))) {
      incrementAfterDestroyCounters(event, EventCountersBB.getBB());
      checkVM();
      checkCallback(event, BridgeNotify.destroyCallbackPrefix);
   }
}

public void afterInvalidate(EntryEvent event) {
   logCall("afterInvalidate", event);
   incrementAfterInvalidateCounters(event, EventCountersBB.getBB());
   checkVM();

   /* We can only verify the callback if we registeredInterest (previously notifyBySubscription).
    * Otherwise, we'll be expecting invalidate callbacks for operations
    * which were creates, etc.
    */
   String clientInterest = TestConfig.tasktab().stringAt( BridgeNotifyPrms.clientInterest, TestConfig.tab().stringAt( BridgeNotifyPrms.clientInterest, null));
   boolean registeredInterest = (clientInterest != null) && (!clientInterest.equalsIgnoreCase("receiveValuesAsInvalidates"));
   if (registeredInterest) {
      checkCallback(event, BridgeNotify.invalidateCallbackPrefix);
   }
}

public void afterUpdate(EntryEvent event) {
   logCall("afterUpdate", event);

   String callbackObj = (String)event.getCallbackArgument();
   if ((callbackObj == null) || ((callbackObj != null) && (!callbackObj.startsWith("ignore")))) {
      incrementAfterUpdateCounters(event, EventCountersBB.getBB());
      checkVM();
      checkCallback(event, BridgeNotify.updateCallbackPrefix);
   }
}

public void afterRegionDestroy(RegionEvent event) {
   logCall("afterRegionDestroy", event);
   checkVM();
}

public void afterRegionInvalidate(RegionEvent event) {
   logCall("afterRegionInvalidate", event);
   checkVM();
}
public void afterRegionClear(RegionEvent event) {
  logCall("afterRegionClear", event);

  }

public void afterRegionCreate(RegionEvent event) {
  logCall("afterRegionCreate", event);
  checkVM();
}

public void afterRegionLive(RegionEvent event) {
  logCall("afterRegionLive", event);
}

public void close() {
   logCall("close", null);
   EventCountersBB.getBB().getSharedCounters().increment(EventCountersBB.numClose);
   checkVM();
}

/** Check that this method is running in the same VM where the listener
 *  was created. Log an error if any problems are detected.
 *
 */
protected void checkVM() {
   int myPID = ProcessMgr.getProcessId();
   if (whereIWasRegistered != myPID) {
      String errStr = "Expected event to be invoked in VM " + whereIWasRegistered +
         ", but it was invoked in " + myPID + ": " + toString() + "; see system.log for call stack";
      throwException(errStr);
   }
}
 
/** Check that the callback object is expected. Log an error if any problems are detected.
 *
 *  @param event The event object.
 *  @param expectedCallbackPrefix - The expected prefix on the callback object (String).
 */
protected void checkCallback(CacheEvent event, String expectedCallbackPrefix) {

   String callbackObj = (String)event.getCallbackArgument();

   // todo@lhughes -- take this out once callbacks bug is fixed
   if (callbackObj == null) {
     return;
   }

   if (isCarefulValidation) {
      if (callbackObj == null) {
         String errStr = "Callback object is " + TestHelper.toString(callbackObj);
         throwException(errStr);
      }
      if (!callbackObj.startsWith(expectedCallbackPrefix)) {
         String errStr = "Expected " + expectedCallbackPrefix + ", but callback object is " + 
                TestHelper.toString(callbackObj);
         throwException(errStr);
      }
   }

   if (callbackObj != null) {

      int myPID = ProcessMgr.getProcessId();
      // An easy workaround to the problem where some pid strings might be equal except
      // for a final digit, as in "pid 501" and "pid 5012". Also, some tests append a space
      // plus more text to the callback string while others end the callback string after
      // the pid. So, appending a space to the callback string here and checking for the 
      // desired pid plus a space should avoid this problem, even if it's not elegant.
/* todo@lhughes - check with gregp about this, maybe local to server/delegate -- remote to initiating client?
      String tmpCallbackObj = callbackObj + " ";
      boolean eventProducedInThisVM = tmpCallbackObj.indexOf("pid " + myPID + " ") >= 0;
      boolean isRemote = event.isOriginRemote();
      if (isRemote == eventProducedInThisVM) { // error with isRemote
         String errStr = "Unexpected event.isOriginRemote() = " + isRemote + ", myPID = " + myPID +
                ", callbackObj showing origination VM = " + callbackObj;
         throwException(errStr);
      }
*/
   }
}

/** 
 * Utility method to write an Exception string to the Event Blackboard and 
 * to also throw an exception containing the same string.
 *
 * @param errStr String to log, post to BridgeNotifyBB and throw
 * @throws TestException containing the passed in String
 *
 * @see util.TestHelper.checkForEventError
 */
protected void throwException(String errStr) {
      hydra.blackboard.SharedMap aMap = EventCountersBB.getBB().getSharedMap();
      aMap.put(TestHelper.EVENT_ERROR_KEY, errStr + " " + TestHelper.getStackTrace());
      Log.getLogWriter().info(errStr);
      throw new TestException(errStr);
}

public void init(java.util.Properties prop) {
   logCall("init(Properties)", null);
}

}
