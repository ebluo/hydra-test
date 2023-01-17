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

import hydra.*;
import util.*;
import org.apache.geode.cache.*;

public class MLRioFDListener extends util.AbstractListener implements CacheListener, Declarable {

public void afterCreate(EntryEvent event) {
   //logCall("afterCreate", event);
}

public void afterDestroy(EntryEvent event) {
   //logCall("afterDestroy", event);
}

public void afterInvalidate(EntryEvent event) {
   //logCall("afterInvalidate", event);
}

public void afterRegionDestroy(RegionEvent event) {
   logCall("afterRegionDestroy", event);
   if (event.getOperation().equals(Operation.FORCED_DISCONNECT)) {
      MLRioBB.getBB().getSharedCounters().increment(MLRioBB.forcedDisconnects);
   }
}

public void afterRegionInvalidate(RegionEvent event) {
   //logCall("afterRegionInvalidate", event);
}

public void afterUpdate(EntryEvent event) {
   //logCall("afterUpdate", event);
}

public void close() {
   //logCall("close", null);
}

public void afterRegionClear(RegionEvent event) {
  //logCall("afterRegionClear", event);
}

public void afterRegionCreate(RegionEvent event) {
  //logCall("afterRegionCreate", event);
}

public void afterRegionLive(RegionEvent event) {
  logCall("afterRegionLive", event);
}

public void init(java.util.Properties prop) {
   logCall("init(Properties)", null);
}

}
