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
package util;

import java.util.*;

import parReg.ParRegPrms;
import hydra.*;
import parReg.tx.ModRoutingObject;

import org.apache.geode.cache.*;
import org.apache.geode.distributed.*;

/**
 * 
 * Utility class to provide doEntryOperations() functionality on a single region.
 * Other classes may extend this class in order to provide basic
 * operation coverage.  
 *
 * @see util.OperationsClientPrms
 *
 * @author Lynn Hughes-Godfrey
 * @since 5.5
 */
public class OperationsClient {

   // operations
   static protected final int ENTRY_ADD_OPERATION = 1;
   static protected final int ENTRY_DESTROY_OPERATION = 2;
   static protected final int ENTRY_INVALIDATE_OPERATION = 3;
   static protected final int ENTRY_LOCAL_DESTROY_OPERATION = 4;
   static protected final int ENTRY_LOCAL_INVALIDATE_OPERATION = 5;
   static protected final int ENTRY_UPDATE_OPERATION = 6;
   static protected final int ENTRY_GET_OPERATION = 7;
   static protected final int ENTRY_GET_NEW_OPERATION = 8;
   static protected final int PUT_IF_ABSENT_OPERATION = 9;
   static protected final int REMOVE_OPERATION = 10;
   static protected final int REPLACE_OPERATION = 11;
   static protected final int ENTRY_PUTALL_OPERATION = 12;

   // lock names
   protected static String LOCK_SERVICE_NAME = "MyLockService";
   protected static String LOCK_NAME = "MyLock_";

   // String prefixes for event callback object
   protected static final String getCallbackPrefix = "Get originated in pid ";
   protected static final String createCallbackPrefix = "Create event originated in pid ";
   protected static final String updateCallbackPrefix = "Update event originated in pid ";
   protected static final String invalidateCallbackPrefix = "Invalidate event originated in pid ";
   protected static final String destroyCallbackPrefix = "Destroy event originated in pid ";
   protected static final String regionInvalidateCallbackPrefix = "Region invalidate event originated in pid ";
   protected static final String regionDestroyCallbackPrefix = "Region destroy event originated in pid ";
   
   protected static final String VmIDStr = "VmId_";

   // instance fields
   protected long minTaskGranularitySec;       // the task granularity in seconds
   protected long minTaskGranularityMS;        // the task granularity in milliseconds
   protected int numOpsPerTask;                // the number of operations to execute per task
   protected RandomValues randomValues =       // instance of random values, used as the value for puts
          new RandomValues();

   protected int upperThreshold;               // value of OperationsClientPrms.upperThreshold
   protected int lowerThreshold;               // value of OperationsClientPrms.lowerThreshold

   protected boolean lockOperations;           // value of OperationsClientPrms.lockOperations
   protected DistributedLockService distLockService; // the distributed lock service for this VM
   protected boolean useTransactions;

  //============================================================================
  // INITTASKS
  //============================================================================

  /**
   * Creates a (disconnected) locator.
   */
  public static void createLocatorTask() {
    DistributedSystemHelper.createLocator();
  }

  /**
   * Connects a locator to its (admin-only) distributed system.
   */
  public static void startAndConnectLocatorTask() {
    DistributedSystemHelper.startLocatorAndAdminDS();
  }

  /**
   * Stops a locator.
   */
  public static void stopLocatorTask() {
    DistributedSystemHelper.stopLocator();
  }

  public void initializeOperationsClient() {
     minTaskGranularitySec = TestConfig.tab().longAt(TestHelperPrms.minTaskGranularitySec, Long.MAX_VALUE);
     if (minTaskGranularitySec == Long.MAX_VALUE)
        minTaskGranularityMS = Long.MAX_VALUE;
     else
        minTaskGranularityMS = minTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
     numOpsPerTask = OperationsClientPrms.numOpsPerTask();

     upperThreshold = TestConfig.tab().intAt(OperationsClientPrms.upperThreshold, Integer.MAX_VALUE);
     lowerThreshold = TestConfig.tab().intAt(OperationsClientPrms.lowerThreshold, -1);

     useTransactions = OperationsClientPrms.useTransactions();

     lockOperations = TestConfig.tab().booleanAt(OperationsClientPrms.lockOperations, false);
     if (lockOperations) {
        Log.getLogWriter().info("Creating lock service " + LOCK_SERVICE_NAME);
        distLockService = DistributedLockService.create(LOCK_SERVICE_NAME, DistributedSystemHelper.getDistributedSystem());
        Log.getLogWriter().info("Created lock service " + LOCK_SERVICE_NAME);
     }
     Log.getLogWriter().info("minTaskGranularitySec " + minTaskGranularitySec + ", " +
                             "minTaskGranularityMS " + minTaskGranularityMS + ", " +
                             "numOpsPerTask " + numOpsPerTask + ", " +
                             "useTransactions " + useTransactions + ", " +
                             "lockOperations " + lockOperations + ", " +
                             "upperThreshold " + upperThreshold + ", " +
                             "lowerThreshold " + lowerThreshold);

    }

   /** Do random entry operations on the given region ending either with
    *  minTaskGranularityMS or numOpsPerTask.
    */
    protected void doEntryOperations(Region aRegion) {
       Log.getLogWriter().info("In doEntryOperations with " + aRegion.getFullPath());

       long startTime = System.currentTimeMillis();
       int numOps = 0;
       boolean rolledback = false;
       
       // In transaction tests, package the operations into a single transaction
       if (useTransactions) {
         TxHelper.begin();
       }

       do {
          int whichOp = getOperation(OperationsClientPrms.entryOperations);
          int size = aRegion.size();
          if (size >= upperThreshold) {
             whichOp = getOperation(OperationsClientPrms.upperThresholdOperations);
          } else if (size <= lowerThreshold) {
             whichOp = getOperation(OperationsClientPrms.lowerThresholdOperations);
          }   

          String lockName = null;
    
          boolean gotTheLock = false;
          if (lockOperations) {
             lockName = LOCK_NAME + TestConfig.tab().getRandGen().nextInt(1, 20);
             Log.getLogWriter().info("Trying to get distributed lock " + lockName + "...");
             gotTheLock = distLockService.lock(lockName, -1, -1);
             if (!gotTheLock) {
                throw new TestException("Did not get lock " + lockName);
             }
             Log.getLogWriter().info("Got distributed lock " + lockName + ": " + gotTheLock);
          }

          try {
             switch (whichOp) {
                case ENTRY_ADD_OPERATION:
                   addEntry(aRegion);
                   break;
                case ENTRY_INVALIDATE_OPERATION:
                   invalidateEntry(aRegion, false);
                   break;
                case ENTRY_DESTROY_OPERATION:
                   destroyEntry(aRegion, false);
                   break;
                case ENTRY_UPDATE_OPERATION:
                   updateEntry(aRegion);
                   break;
                case ENTRY_GET_OPERATION:
                   getKey(aRegion);
                   break;
                case ENTRY_GET_NEW_OPERATION:
                   getNewKey(aRegion);
                   break;
                case ENTRY_LOCAL_INVALIDATE_OPERATION:
                   invalidateEntry(aRegion, true);
                   break;
                case ENTRY_LOCAL_DESTROY_OPERATION:
                   destroyEntry(aRegion, true);
                   break;
                case PUT_IF_ABSENT_OPERATION:
                    putIfAbsent(aRegion, true);
                    break;
                case REMOVE_OPERATION:
                    remove(aRegion);
                    break;
                case REPLACE_OPERATION:
                    replace(aRegion);
                    break;
                case ENTRY_PUTALL_OPERATION:
                  putAll(aRegion);
                  break;
                default: {
                   throw new TestException("Unknown operation " + whichOp);
                }
            }
          } catch (TransactionDataNodeHasDepartedException e) {
            if (!useTransactions) {
              throw new TestException("Unexpected TransactionDataNodeHasDepartedException " + TestHelper.getStackTrace(e));
            } else {
              Log.getLogWriter().info("Caught TransactionDataNodeHasDepartedException.  Expected with concurrent execution, continuing test.");
              Log.getLogWriter().info("Rolling back transaction.");
              try {
                rolledback = true;
                TxHelper.rollback();
                Log.getLogWriter().info("Done Rolling back Transaction");
              } catch (TransactionException te) {
                Log.getLogWriter().info("Caught exception " + te + " on rollback() after catching TransactionDataNodeHasDeparted during tx ops.  Expected, continuing test.");
              }
            }
          } catch (TransactionDataRebalancedException e) {
            if (!useTransactions) {
              throw new TestException("Unexpected Exception " + e + ". " + TestHelper.getStackTrace(e));
            } else {
              Log.getLogWriter().info("Caught Exception " + e + ".  Expected with concurrent execution, continuing test.");
              Log.getLogWriter().info("Rolling back transaction.");
              rolledback = true;
              TxHelper.rollback();
              Log.getLogWriter().info("Done Rolling back Transaction");
            }
         } finally {
            if (gotTheLock) {
               gotTheLock = false;
               distLockService.unlock(lockName);
               Log.getLogWriter().info("Released distributed lock " + lockName);
            }
         }
         numOps++;
         Log.getLogWriter().info("Completed op " + numOps + " for this task");
      } while ((System.currentTimeMillis() - startTime < minTaskGranularityMS) &&
               (numOps < numOpsPerTask));

       // finish transactions (commit or rollback)
       if (useTransactions && !rolledback) {
          int n = 0;
          int commitPercentage = OperationsClientPrms.getCommitPercentage();
          n = TestConfig.tab().getRandGen().nextInt(1, 100);

          if (n <= commitPercentage) {
            try {
               TxHelper.commit();
            } catch (TransactionDataNodeHasDepartedException e) {
              Log.getLogWriter().info("Caught TransactionDataNodeHasDepartedException.  Expected with concurrent execution, continuing test.");
            } catch (TransactionInDoubtException e) {
              Log.getLogWriter().info("Caught TransactionInDoubtException.  Expected with concurrent execution, continuing test.");
            } catch (ConflictException e) {
               Log.getLogWriter().info("ConflictException " + e + " expected, continuing test");
            }
          } else {
              TxHelper.rollback();
          }
       }
       Log.getLogWriter().info("Done in doEntryOperations with " + aRegion.getFullPath());
    }

   /** Add a new entry to the given region.
    *
    *  @param aRegion The region to use for adding a new entry.
    *
    */
   protected void addEntry(Region aRegion) {
      Object key = getNewKey();
      BaseValueHolder anObj = getValueForKey(key);
      String callback = createCallbackPrefix + ProcessMgr.getProcessId();
      if (TestConfig.tab().getRandGen().nextBoolean()) { // use a create call
         if (TestConfig.tab().getRandGen().nextBoolean()) { // use a create call with cacheWriter arg
            Log.getLogWriter().info("addEntry: calling create for key " + key + ", object " +
               TestHelper.toString(anObj) + " cacheWriterParam is " + callback + ", region is " + 
               aRegion.getFullPath());
            aRegion.create(key, anObj, callback);
            Log.getLogWriter().info("addEntry: done creating key " + key);
         } else { // use create with no cacheWriter arg
            Log.getLogWriter().info("addEntry: calling create for key " + key + ", object " +
               TestHelper.toString(anObj) + ", region is " + aRegion.getFullPath());
            aRegion.create(key, anObj);
            Log.getLogWriter().info("addEntry: done creating key " + key);
         }
      } else { // use a put call
         if (TestConfig.tab().getRandGen().nextBoolean()) { // use a put call with callback arg
            Log.getLogWriter().info("addEntry: calling put for key " + key + ", object " +
                  TestHelper.toString(anObj) + " callback is " + callback + ", region is " + aRegion.getFullPath());
            aRegion.put(key, anObj, callback);
            Log.getLogWriter().info("addEntry: done putting key " + key);
         } else {
            Log.getLogWriter().info("addEntry: calling put for key " + key + ", object " +
                  TestHelper.toString(anObj) + ", region is " + aRegion.getFullPath());
            aRegion.put(key, anObj);
            Log.getLogWriter().info("addEntry: done putting key " + key);
         }
      }
   }

   /** 
    *  ConcurrentMap API testing
    */
   protected void putIfAbsent(Region aRegion, boolean logAddition) {
       Object key = null;

       // Expect success most of the time (put a new entry into the cache)
       int randInt = TestConfig.tab().getRandGen().nextInt(1, 100);
       if (randInt <= 25) {
          Set aSet = aRegion.keySet();
          if (aSet.size() > 0) {
            Iterator it = aSet.iterator();
            if (it.hasNext()) {
                key = it.next();
            } 
         }
       }

       if (key == null) {
          key = getNewKey();
       }
       Object anObj = getValueForKey(key);

       if (logAddition) {
           Log.getLogWriter().info("putIfAbsent: calling putIfAbsent for key " + key + ", object " + TestHelper.toString(anObj) + ", region is " + aRegion.getFullPath() + ".");
       }

       Object prevVal = null;
       prevVal = aRegion.putIfAbsent(key, anObj);
   }
    
   /** putall a map to the given region.
   *
   *  @param aRegion The region to use for putall a map.
   *
   */
   protected void putAll(Region r) {
      // determine the number of new keys to put in the putAll
      int beforeSize = r.size();
      String numPutAllNewKeys = TestConfig.tab().stringAt(ParRegPrms.numPutAllNewKeys);
      int numNewKeysToPut = 0;
      if (numPutAllNewKeys.equalsIgnoreCase("useThreshold")) {
         numNewKeysToPut = upperThreshold - beforeSize; 
         if (numNewKeysToPut <= 0) {
            numNewKeysToPut = 1;
         } else {
            int max = TestConfig.tab().intAt(ParRegPrms.numPutAllMaxNewKeys,
                                                        numNewKeysToPut);
            max = Math.min(numNewKeysToPut, max);
            int min = TestConfig.tab().intAt(ParRegPrms.numPutAllMinNewKeys, 1);
            min = Math.min(min, max);
            numNewKeysToPut = TestConfig.tab().getRandGen().nextInt(min, max);
         }
      } else {
         numNewKeysToPut = Integer.valueOf(numPutAllNewKeys).intValue();
      }

      // get a map to put
      Map mapToPut = null;
      int randInt = TestConfig.tab().getRandGen().nextInt(1, 100);
      if (randInt <= 25) {
         mapToPut = new HashMap();
      } else if (randInt <= 50) {
         mapToPut = new Hashtable();
      } else if (randInt <= 75) {
         mapToPut = new TreeMap();
      } else {
         mapToPut = new LinkedHashMap();
      }

      // add new keys to the map
      StringBuffer newKeys = new StringBuffer();
      for (int i = 1; i <= numNewKeysToPut; i++) { // put new keys
         Object key = getNewKey();
         BaseValueHolder anObj = getValueForKey(key);
         mapToPut.put(key, anObj);
         newKeys.append(key + " ");
         if ((i % 10) == 0) {
            newKeys.append("\n");
         }
      }

      // add existing keys to the map
      int numPutAllExistingKeys = TestConfig.tab().intAt(ParRegPrms.numPutAllExistingKeys);
      List keyList = getExistingKeys(r, numPutAllExistingKeys);
      StringBuffer existingKeys = new StringBuffer();
      if (keyList.size() != 0) { // no existing keys could be found
         for (int i = 0; i < keyList.size(); i++) { // put existing keys
            Object key = keyList.get(i);
            BaseValueHolder anObj = getUpdateObject(r, key);
            mapToPut.put(key, anObj);
            existingKeys.append(key + " ");
            if (((i+1) % 10) == 0) {
               existingKeys.append("\n");
            }
         }
      }
      Log.getLogWriter().info("Region size is " + r.size() + ", map to use as argument to putAll is " + 
          mapToPut.getClass().getName() + " containing " + numNewKeysToPut + " new keys and " + 
          keyList.size() + " existing keys (updates); total map size is " + mapToPut.size() +
          "\nnew keys are: " + newKeys + "\n" + "existing keys are: " + existingKeys);
      for (Object key: mapToPut.keySet()) {
         Log.getLogWriter().info("putAll map key " + key + ", value " + TestHelper.toString(mapToPut.get(key)));
      }

      // do the putAll
      Log.getLogWriter().info("putAll: calling putAll with map of " + mapToPut.size() + " entries");
      r.putAll(mapToPut);
      
      Log.getLogWriter().info("putAll: done calling putAll with map of " + mapToPut.size() + " entries");

   }

   /** Invalidate an entry in the given region.
    *
    *  @param aRegion The region to use for invalidating an entry.
    *  @param isLocalInvalidate True if the invalidate should be local, false otherwise.
    */
   protected void invalidateEntry(Region aRegion, boolean isLocalInvalidate) {
      Object key = getExistingKey(aRegion);
      if (key == null) {
         return;
      }
      try {
         String callback = invalidateCallbackPrefix + ProcessMgr.getProcessId();
         if (isLocalInvalidate) { // do a local invalidate
            if (TestConfig.tab().getRandGen().nextBoolean()) { // local invalidate with callback
               Log.getLogWriter().info("invalidateEntry: local invalidate for " + key + " callback is " + callback);
               aRegion.localInvalidate(key, callback);
               Log.getLogWriter().info("invalidateEntry: done with local invalidate for " + key);
            } else { // local invalidate without callback
               Log.getLogWriter().info("invalidateEntry: local invalidate for " + key);
               aRegion.localInvalidate(key);
               Log.getLogWriter().info("invalidateEntry: done with local invalidate for " + key);
            }
         } else { // do a distributed invalidate
            if (TestConfig.tab().getRandGen().nextBoolean()) { // invalidate with callback
               Log.getLogWriter().info("invalidateEntry: invalidating key " + key + " callback is " + callback);
               aRegion.invalidate(key, callback);
               Log.getLogWriter().info("invalidateEntry: done invalidating key " + key);
            } else { // invalidate without callback
               Log.getLogWriter().info("invalidateEntry: invalidating key " + key);
               aRegion.invalidate(key);
               Log.getLogWriter().info("invalidateEntry: done invalidating key " + key);
            }
         }
      } catch (com.gemstone.gemfire.cache.EntryNotFoundException e) {
         Log.getLogWriter().info("Caught " + e + " (expected with concurrent execution); continuing with test");
         return;
      }
   }
       
   /** Destroy an entry in the given region.
    *
    *  @param aRegion The region to use for destroying an entry.
    *  @param isLocalDestroy True if the destroy should be local, false otherwise.
    */
   protected void destroyEntry(Region aRegion, boolean isLocalDestroy) {
      Object key = getExistingKey(aRegion);
      if (key == null) {
         int size = aRegion.size();
         return;
      }
      try {
         String callback = destroyCallbackPrefix + ProcessMgr.getProcessId();
         if (isLocalDestroy) { // do a local destroy
            if (TestConfig.tab().getRandGen().nextBoolean()) { // local destroy with callback
               Log.getLogWriter().info("destroyEntry: local destroy for " + key + " callback is " + callback);
               aRegion.localDestroy(key, callback);
               Log.getLogWriter().info("destroyEntry: done with local destroy for " + key);
            } else { // local destroy without callback
               Log.getLogWriter().info("destroyEntry: local destroy for " + key);
               aRegion.localDestroy(key);
               Log.getLogWriter().info("destroyEntry: done with local destroy for " + key);
            }
         } else { // do a distributed destroy
            if (TestConfig.tab().getRandGen().nextBoolean()) { // destroy with callback
               Log.getLogWriter().info("destroyEntry: destroying key " + key + " callback is " + callback);
               aRegion.destroy(key, callback);
               Log.getLogWriter().info("destroyEntry: done destroying key " + key);
            } else { // destroy without callback
               Log.getLogWriter().info("destroyEntry: destroying key " + key);
               aRegion.destroy(key);
               Log.getLogWriter().info("destroyEntry: done destroying key " + key);
            }
         }
      } catch (com.gemstone.gemfire.cache.EntryNotFoundException e) {
         Log.getLogWriter().info("Caught " + e + " (expected with concurrent execution); continuing with test");
         return;
      }
   }


   /**
    *  ConcurrentMap API testing
    **/  
   protected void remove(Region aRegion) {
       Set aSet = aRegion.keys();
       Iterator iter = aSet.iterator();
       if (!iter.hasNext()) {
           Log.getLogWriter().info("remove: No names in region");
           return;
       }
       try {
           Object name = iter.next();
           Object oldVal = aRegion.get(name);
           remove(aRegion, name, oldVal);
       } catch (NoSuchElementException e) {
           throw new TestException("Bug 30171 detected: " + TestHelper.getStackTrace(e));
       }
   }
       
   protected void remove(Region aRegion, Object name, Object oldVal) {

       boolean removed;
       try {

         // Force the condition to not be met (small percentage of the time)
         int randInt = TestConfig.tab().getRandGen().nextInt(1, 100);
         if (randInt <= 25) {
           oldVal = getUpdateObject(aRegion, name);
          }

          Log.getLogWriter().info("remove: removing " + name + " with previous value " + oldVal + ".");
          removed = aRegion.remove(name, oldVal);
          Log.getLogWriter().info("remove: done removing " + name);
       } catch (com.gemstone.gemfire.cache.EntryNotFoundException e) {
          Log.getLogWriter().info("Caught " + e + " (expected with concurrent execution); continuing with test");
          return;
       }
   }
    
   /** Update an existing entry in the given region. If there are
    *  no available keys in the region, then this is a noop.
    *
    *  @param aRegion The region to use for updating an entry.
    */
   protected void updateEntry(Region aRegion) {
      Object key = getExistingKey(aRegion);
      if (key == null) {
         int size = aRegion.size();
         return;
      }
      BaseValueHolder anObj = getUpdateObject(aRegion, key);
      String callback = updateCallbackPrefix + ProcessMgr.getProcessId();
      if (TestConfig.tab().getRandGen().nextBoolean()) { // do a put with callback arg
         Log.getLogWriter().info("updateEntry: replacing key " + key + " with " +
            TestHelper.toString(anObj) + ", callback is " + callback);
         aRegion.put(key, anObj, callback);
         Log.getLogWriter().info("Done with call to put (update)");
      } else { // do a put without callback
         Log.getLogWriter().info("updateEntry: replacing key " + key + " with " + TestHelper.toString(anObj));
         aRegion.put(key, anObj);
         Log.getLogWriter().info("Done with call to put (update)");
      }
   }


   /**
    * Updates the "first" entry in a given region
    */
   protected void replace(Region aRegion) {
       Set aSet = aRegion.keys();
       Iterator iter = aSet.iterator();
       if (!iter.hasNext()) {
           Log.getLogWriter().info("replace: No names in region");
           return;
       }
       Object name = iter.next();
       replace(aRegion, name);
   }
       
   /**
    * Updates the entry with the given key (<code>name</code>) in the
    * given region.
    */
   protected void replace(Region aRegion, Object name) {
       Object anObj = null;
       Object prevVal = null;
       boolean replaced = false;
       try {
           anObj = aRegion.get(name);
       } catch (CacheLoaderException e) {
           throw new TestException(TestHelper.getStackTrace(e));
       } catch (TimeoutException e) {
           throw new TestException(TestHelper.getStackTrace(e));
       }

       Object newObj = getUpdateObject(aRegion, name);
       // 1/2 of the time use oldVal => newVal method
       if (TestConfig.tab().getRandGen().nextBoolean()) {
          if (TestConfig.tab().getRandGen().nextBoolean()) {
             // Force the condition to not be met 
             // get a new oldVal to cause this
            anObj = getUpdateObject(aRegion, name);
          }

          Log.getLogWriter().info("replace: replacing name " + name + " with " + TestHelper.toString(newObj) + "; old value is " + TestHelper.toString(anObj) + ".");
           replaced = aRegion.replace(name, anObj, newObj);
       } else {

          if (TestConfig.tab().getRandGen().nextBoolean()) {
            // Force the condition to not be met
            // use a new key for this
            name = getNewKey();
          }
          Log.getLogWriter().info("replace: replacing name " + name + " with " + TestHelper.toString(newObj) + ".");
          prevVal = aRegion.replace(name, newObj);

          if (prevVal != null) replaced = true;
       }
       Log.getLogWriter().info("Done with call to replace");
   }
       
   /** Get an existing key in the given region if one is available,
    *  otherwise get a new key. 
    *
    *  @param aRegion The region to use for getting an entry.
    */
   protected void getKey(Region aRegion) {
      Object key = getExistingKey(aRegion);
      if (key == null) { // no existing keys; get a new key then
         int size = aRegion.size();
         return;
      }
      String callback = getCallbackPrefix + ProcessMgr.getProcessId();
      Object anObj = null;
      if (TestConfig.tab().getRandGen().nextBoolean()) { // get with callback
         Log.getLogWriter().info("getKey: getting key " + key + ", callback is " + callback);
         anObj = aRegion.get(key, callback);
         Log.getLogWriter().info("getKey: got value for key " + key + ": " + TestHelper.toString(anObj));
      } else { // get without callback
         Log.getLogWriter().info("getKey: getting key " + key);
         anObj = aRegion.get(key);
         Log.getLogWriter().info("getKey: got value for key " + key + ": " + TestHelper.toString(anObj));
      }
   }
       
   /** Get a new key int the given region.
    *
    *  @param aRegion The region to use for getting an entry.
    */
   protected void getNewKey(Region aRegion) {
      Object key = getNewKey();
      String callback = getCallbackPrefix + ProcessMgr.getProcessId();
      int beforeSize = aRegion.size();
      Object anObj = null;
      if (TestConfig.tab().getRandGen().nextBoolean()) { // get with callback
         Log.getLogWriter().info("getNewKey: getting new key " + key + ", callback is " + callback);
         anObj = aRegion.get(key, callback);
      } else { // get without callback
         Log.getLogWriter().info("getNewKey: getting new key " + key);
         anObj = aRegion.get(key);
      }
      Log.getLogWriter().info("getNewKey: done getting value for new key " + key + ": " + TestHelper.toString(anObj));
   }

   // ========================================================================
   // helper methods 

   /** Return a value for the given key
    */
   public BaseValueHolder getValueForKey(Object key) {
     String name = null;
     if (key instanceof ModRoutingObject) {
       ModRoutingObject routingObject = (ModRoutingObject)key;
       name = (String)routingObject.getKey();
     } else {
       name = (String)key;
     }
     return new ValueHolder(name, randomValues);
   }

   /** Return a new key, never before used in the test.
    */
   protected Object getNewKey() {
      Object key =  NameFactory.getNextPositiveObjectName();
      return key;
   }
       
   /** Return a random recently used key.
    *
    *  @param aRegion The region to use for getting a recently used key.
    *  @param recentHistory The number of most recently used keys to consider
    *         for returning.
    *
    *  @returns A recently used key, or null if none.
    */
   protected Object getRecentKey(Region aRegion, int recentHistory) {
      long maxNames = NameFactory.getPositiveNameCounter();
      if (maxNames <= 0) {
         return null;
      }
      long keyIndex = TestConfig.tab().getRandGen().nextLong(
                         Math.max(maxNames-recentHistory, (long)1), 
                         maxNames);
      Object key = NameFactory.getObjectNameForCounter(keyIndex);
      return key;
   }
   
   /** Return an object to be used to update the given key. If the
    *  value for the key is a ValueHolder, then get an alternate
    *  value which is similar to it's previous value (see
    *  ValueHolder.getAlternateValueHolder()).
    *
    *  @param aRegion The region which possible contains key.
    *  @param key The key to get a new value for.
    *  
    *  @returns An update to be used to update key in aRegion.
    */
   protected BaseValueHolder getUpdateObject(Region aRegion, Object key) {
      BaseValueHolder anObj = (BaseValueHolder)aRegion.get(key);
      BaseValueHolder newObj = null;
      if (anObj == null) {
        //TODO change the ValueHolder constructor to take into
        // account different parameters
        if (key instanceof String) {
          newObj = new ValueHolder((String)key, randomValues);
        } else {
          newObj = new ValueHolder(key, randomValues);
        }
      } else {
        newObj = anObj.getAlternateValueHolder(randomValues);
      }
      return newObj;
   }
   
   /** Get a random operation using the given hydra parameter.
    *
    *  @param whichPrm A hydra parameter which specifies random operations.
    *
    *  @returns A random operation.
    */
   protected int getOperation(Long whichPrm) {
      int op = 0;
      String operation = TestConfig.tab().stringAt(whichPrm);
      if (operation.equals("add"))
         op = ENTRY_ADD_OPERATION;
      else if (operation.equals("update"))
         op = ENTRY_UPDATE_OPERATION;
      else if (operation.equals("invalidate"))
         op = ENTRY_INVALIDATE_OPERATION;
      else if (operation.equals("destroy"))
         op = ENTRY_DESTROY_OPERATION;
      else if (operation.equals("get"))
         op = ENTRY_GET_OPERATION;
      else if (operation.equals("getNew"))
         op = ENTRY_GET_NEW_OPERATION;
      else if (operation.equals("localInvalidate"))
         op = ENTRY_LOCAL_INVALIDATE_OPERATION;
      else if (operation.equals("localDestroy"))
         op = ENTRY_LOCAL_DESTROY_OPERATION;
      else if (operation.equals("putIfAbsent"))
         op = PUT_IF_ABSENT_OPERATION;
      else if (operation.equals("remove"))
         op = REMOVE_OPERATION;
      else if (operation.equals("replace"))
         op = REPLACE_OPERATION;
      else if (operation.equals("putAll"))
        op = ENTRY_PUTALL_OPERATION;
      else
         throw new TestException("Unknown entry operation: " + operation);
      return op;
   }
   
   /** Return a random key currently in the given region.
    *
    *  @param aRegion The region to use for getting an existing key (may
    *         or may not be a partitioned region).
    *
    *  @returns A key in the region.
    */
   protected Object getExistingKey(Region aRegion) {
      Object key = null;
      Object[] keyList = aRegion.keySet().toArray();
      int index = TestConfig.tab().getRandGen().nextInt(0, keyList.length-1);
      if (index > 0) {
         key = keyList[index];
      }
      return key;
   }
   
   protected List getExistingKeys(Region aRegion, int numKeysToGet) {
     Log.getLogWriter().info("Trying to get " + numKeysToGet + " existing keys...");
     List keyList = new ArrayList();
     Set aSet = aRegion.keySet();
     Iterator it = aSet.iterator();
     while (it.hasNext()) {
       keyList.add(it.next());
       if (keyList.size() >= numKeysToGet) {
         return keyList;
       }
     }
     return keyList;
   }
}
