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
package tx; 

import parReg.tx.PrTxBB;  
import util.*;
import hydra.*;
import org.apache.geode.cache.*;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.distributed.DistributedMember;

import java.util.*;

/**
 * A listener for handling transaction related events.  Note that RegionEvents
 * are not transactional and are not handled by this listener.  This is because
 * they occur at the time of execution rather than at commit time.
 *
 * @author lhughes
 * @see TransactionListener
 */
public class PrTxListener extends util.AbstractListener implements TransactionListener {

/** Called after a successful commit of a transaction: logs and validates event
 *
 * @param event the TransactionEvent
 */
public void afterCommit(TransactionEvent event) {
  logTxEvent("afterCommit", event);
  TxBB.inc(TxBB.TX_SUCCESS);

  validate(TxBB.TXACTION_COMMIT, event);
}

/** Called after an explicit rollback of a transaction: logs and validates event
 *
 * @param event the TransactionEvent
 */
public void afterRollback(TransactionEvent event) {
  logTxEvent("afterRollback", event);
  TxBB.inc(TxBB.TX_ROLLBACK);

  // Now validate this event list with what the cacheListeners recorded
  validate(TxBB.TXACTION_ROLLBACK, event);
}

/** Called after an unsucessful commit operation: logs and validates event
 *
 * @param event the TransactionEvent
 */
public void afterFailedCommit(TransactionEvent event) {
  logTxEvent("afterFailedCommit", event);
  TxBB.inc(TxBB.TX_FAILURE);

  // Now validate this event list with what the cacheListeners recorded
  // afterFailedCommit should have same behavior as afterRollback
  validate(TxBB.TXACTION_ROLLBACK, event);
}

/**  Called when the region containing this callback is destroyed, when the 
 *   cache is closed, or when a callback is removed from a region using an
 *   <code>AttributesMutator</code>.
 */
public void close() {
  Log.getLogWriter().info("PrTxListener: close()");
}

/** Utility method to track counters and validate TransactionEvents.
 *  Validation includes rebuilding original operation list from Set of ordered
 *  events and comparing that list to both the original operation list 
 *  as well as the cacheListener operationList (both saved on the TxBB).
 *
 *  @param txEvent the TransactionEvent to validate
 *
 *  @see TxBB.opListKey
 *  @see TxBB.LocalListenerOpListPrefix
 *  @see TxBB.RemoteListenerOpListPrefix
 */
private void validate(String txAction, TransactionEvent txEvent) {
  Log.getLogWriter().fine("PrTxListener: validate() invoked with TxEvent " + txEvent.toString());

  long txid = TxBB.getBB().getSharedCounters().read(TxBB.TX_NUMBER);
  List txEventList = txEvent.getEvents();

  // convert events to Operations (OpList)
  OpList eventOpList = new OpList();
  for (Iterator it = txEventList.iterator(); it.hasNext();) {
    EntryEvent event = (EntryEvent)it.next();

    // convert event.getOperation() -> opName (create, destroy, invalidate, put)
    com.gemstone.gemfire.cache.Operation eventOp = event.getOperation();
    String opName = null;
    if (eventOp.isCreate()) {
      opName = Operation.ENTRY_CREATE;
      updateTxEventCounter("Create", event);
    } else if (eventOp.isUpdate()) {
      opName = Operation.ENTRY_UPDATE;
      updateTxEventCounter("Update", event);
    } else if (eventOp.isDestroy()) {
      opName = "destroy";
      opName = Operation.ENTRY_DESTROY;
      updateTxEventCounter("Destroy", event);
    } else if (eventOp.isInvalidate()) {
      opName = Operation.ENTRY_INVAL;
      updateTxEventCounter("Invalidate", event);
    }

    // update the opName (include local if needed in entry-destroy & entry-invalidates)
    if (!event.isDistributed()) {
      RegionAttributes ratts = event.getRegion().getAttributes();
      boolean scopeLocal = (ratts.getScope() == Scope.LOCAL) ? true : false;

      if (!scopeLocal) {
        if (opName.equals(Operation.ENTRY_DESTROY)) {
            opName = Operation.ENTRY_LOCAL_DESTROY;
          }
        if (opName.equals(Operation.ENTRY_INVAL)) {
          opName = Operation.ENTRY_LOCAL_INVAL;
        }
      }
    }

    // if we did a load, was it a create of a new key or update of an existing key?
    if (event.isLoad()) {
      if (eventOp.equals(com.gemstone.gemfire.cache.Operation.LOCAL_LOAD_CREATE)) {
        opName = Operation.ENTRY_CREATE;
      } else if (eventOp.equals(com.gemstone.gemfire.cache.Operation.LOCAL_LOAD_UPDATE)) { 
        opName = Operation.ENTRY_UPDATE;
      }
    }
    
    Object oldValue = event.getOldValue();
    if (oldValue instanceof BaseValueHolder) {
      oldValue = ((BaseValueHolder)oldValue).modVal;
    }

    Object newValue = event.getNewValue();
    if (newValue instanceof BaseValueHolder) {
      newValue = ((BaseValueHolder)newValue).modVal;
    }

    Operation op = new Operation(event.getRegion().getFullPath(),
                                event.getKey(),
                                opName,
                                oldValue,
                                newValue);
    eventOpList.add(op);
  }

  // Note that its possible to have a TX COMMIT with no entry ops if
  // we randomly selected region operations only
  if (eventOpList.numOps() > 0) {
    String key = null;
    if (TxUtil.inTxVm()) {
      key = TxBB.LocalListenerOpListPrefix + txid;
    } else {   
      key = TxBB.RemoteListenerOpListPrefix + txid;
    }

    OpList cacheListenerOpList = (OpList)TxBB.getBB().getSharedMap().get(key);

    String opListKey = (String)TxBB.getBB().getSharedMap().get(TxBB.TX_OPLIST_KEY);
    OpList originalOpList = (OpList)TxBB.getBB().getSharedMap().get(opListKey);
    OpList collapsedOpList = originalOpList.collapseForEvents(txAction);

    if (TxUtil.inTxVm()) {
      // For the local/TxVm -- compare collapsed vs. txOps 
      // and entryOps vs. non-collapsed cacheListenerOps
      Log.getLogWriter().info("checking order of operations vs. txEvents");
      checkEventOrder(collapsedOpList, eventOpList);
/* @todo:lhughes (once darrel implements cacheListener event ordering)
      Log.getLogWriter().info("checking order of operations vs. cacheListenerEvents");
      checkEventOrder(entryOpList, cacheListenerOpList);
*/
    } else {
      // For the remote VMs -- compare collapsed entryOps to 
      // tx & collapsed CacheListener opLists
      Log.getLogWriter().info("checking order of operations vs. txEvents");
      checkEventOrder(collapsedOpList, eventOpList);
/* @todo:lhughes (once darrel implements cacheListener event ordering)
      Log.getLogWriter().info("checking order of operations vs. cacheListenerEvents");
      checkEventOrder(collapsedOpList, cacheListenerOpList.collapse());
*/
    }
  }
}


/** 
 *  Compares originalOpList generated by executing commands and saved
 *  to the BB against an event list re-generated from events arriving
 *  at either the CacheListener or TransactionListener.
 *
 *  @param entryOpList opList generated while executing ops
 *  @param eventOpList opList generated by processing either CacheListener
 *                     or TransactionEvents
 *  @throws TestException if difference discovered in lists
 */
protected void checkEventOrder(OpList entryOpList, OpList eventOpList) {

    /* If there are putAll ops, the individual ops within putAll will be unordered
     * so do not do this validation.
     */

    if (!TxPrms.checkTxEventOrder()) {
       return;
    }

    /* Get rid of any original operations that don't cause events to be 
     * generated (e.g. gets where oldValue == newValue (no load))
     */
    OpList newOpList = new OpList();
    for (int i=0; i < entryOpList.numOps(); i++) {
      boolean addOp = true;
      Operation op = entryOpList.getOperation(i);
      if (op.isGetOperation()) { 
        if ((op.getOldValue() == null)) {
          // we must have done a load to create a new key
          op.setOpName(Operation.ENTRY_CREATE);
        } else if (op.getOldValue().toString().equals("INVALID")) {
          // we did a load to create a new value for an existing key
          op.setOpName(Operation.ENTRY_UPDATE);
        } else if (!op.getOldValue().equals(op.getNewValue())) {
          // we could have invalidated an entry, then done a getWithExistingKey (val1 -> invalid -> val2)
          op.setOpName(Operation.ENTRY_UPDATE);
        } else { 
          // a simple get ... no event was sent, we don't want for comparison
          addOp = false;
        }
      } else if (op.isDoubleInvalidate()) {
          // noop: no state change, no event
          Log.getLogWriter().info("Not adding " + op.toString() + " : double invalidate");
          addOp = false;
      }

      // for entries in PartitionedRegions filter out ops for entries not in this VM
      Region aRegion = RegionHelper.getRegion(op.getRegionName());
      if (PartitionRegionHelper.isPartitionedRegion(aRegion)) { 
        DistributedMember dm = DistributedSystemHelper.getDistributedSystem().getDistributedMember();
        boolean alwaysFireLocalListeners = ((Boolean)PrTxBB.getBB().getSharedMap().get(PrTxBB.alwaysFireLocalListeners)).booleanValue();
        int redundantCopies = ((Integer)PrTxBB.getBB().getSharedMap().get(PrTxBB.redundantCopies)).intValue();
      
        if (alwaysFireLocalListeners && (redundantCopies > 0)) {
          Set dataHosts = PartitionRegionHelper.getAllMembersForKey(aRegion, op.getKey());
          if (!dataHosts.contains(dm)) {
            Log.getLogWriter().info("Not adding " + op.toString() + " this VM does not host this entry");
            addOp = false;
          }  
        } else {   // we should only fire this event on the primary
          DistributedMember primaryDM = PartitionRegionHelper.getPrimaryMemberForKey(aRegion, op.getKey());
          if (primaryDM != dm) {
            Log.getLogWriter().info("Not adding " + op.toString() + " this VM does not host this entry");
            addOp = false;
          }
        }
      }
      if (addOp) newOpList.add(op);
    }

    Log.getLogWriter().info("Event OpList operations = " + eventOpList.toString());
    Log.getLogWriter().info("Original OpList operations = " + newOpList.toString());
  
  // If the sizes of the two lists don't match, then we can go ahead and
  // report a problem (avoid ArrayIndexOutOfBoundsException)
  if (newOpList.numOps() != eventOpList.numOps()) {
      StringBuffer aStr = new StringBuffer();
      aStr.append("eventOpList and opList have different sizes, expected to contain same number of entries (and same operations)\n");
      aStr.append("\tEventOpList = " + eventOpList.toString() + "\n");
      aStr.append("\tOpList = " + newOpList.toString());
      throwException(aStr.toString());
  }

  // Validate order of TxEvents & CacheListener events against original ops!
  for (int i = 0; i < newOpList.numOps(); i++) {
    Operation eventOp = eventOpList.getOperation(i);
    Operation op = newOpList.getOperation(i);

    // original op vs. TxEvent operation
    if (!op.equalsEventOp(eventOp)) {
      String errStr = "Original operation: <" + op.toString() + "> does not match eventOp <" + eventOp.toString() + ">";
      throwException(errStr);
    } 
  }
}

/** Update TxEvent Counters based on incoming event
 *
 *  @param eventName string  representing event (create, put, etc)
 *  @param event EntryEvent to include in counts
 */
private void updateTxEventCounter(String eventName, EntryEvent event) {
    TxEventCountersBB.incrementEntryTxEventCntrs(eventName, 
                                       event.isDistributed(),
                                       event.isExpiration(),
                                       event.isOriginRemote(),
                                       event.isLoad(),
                                       event.isLocalLoad(),
                                       event.isNetLoad(),
                                       event.isNetSearch());
}

/** Utility method to log an Exception, place it into a wellknown location in the 
 *  EventBB and throw an exception.
 *
 *  @param errString string to be logged, placed on EventBB and include in
 *         Exception
 *  @throws TestException containing the given string
 *  @see TestHelper.checkForEventError
 */
protected void throwException(String errStr) {
      StringBuffer qualifiedErrStr = new StringBuffer();
      qualifiedErrStr.append("Exception reported in " + RemoteTestModule.getMyClientName() + "\n");
      qualifiedErrStr.append(errStr);
      errStr = qualifiedErrStr.toString();
     
      hydra.blackboard.SharedMap aMap = TxBB.getBB().getSharedMap();
      aMap.put(TestHelper.EVENT_ERROR_KEY, errStr + " " + TestHelper.getStackTrace());         
      Log.getLogWriter().info(errStr);
      throw new TestException(errStr);
}

}

