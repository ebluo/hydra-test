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
package capCon; 

import util.*;
import org.apache.geode.cache.*;

public class EntryEventMemLRUTest extends event.EventTest {

public synchronized static void HydraTask_initialize() {
   if (eventTest == null) {
      eventTest = new EntryEventMemLRUTest();
      ((EntryEventMemLRUTest)eventTest).initialize();
   }
}

protected void initialize() {
   super.initialize();
}

public static void HydraTask_doEntryOperations() {
   Region rootRegion = CacheUtil.getCache().getRegion(eventTest.regionName);
   ((EntryEventMemLRUTest)eventTest).doEntryOperations(rootRegion);
}

protected void doEntryOperations(Region aRegion) {
   super.doEntryOperations(aRegion);
}

public Object getObjectToAdd(String name) {
   Object anObj = randomValues.getRandomObjectGraph();
   return anObj;
}

public Object getUpdateObject(String name) {
   Object anObj = randomValues.getRandomObjectGraph();
   return anObj;
}
}
