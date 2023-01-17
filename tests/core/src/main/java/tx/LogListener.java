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

//import util.*;
//import hydra.*;
//import hydra.blackboard.SharedCounters;
import java.util.Properties;

import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.RegionEvent;

/** 
 *  Tx Test Listener (just for display of CacheListener events)
 */
public class LogListener extends util.AbstractListener implements CacheListener,Declarable {

  public void init(Properties p) {
    
  }
public void afterCreate(EntryEvent event) {
   logCall("afterCreate", event);
}

public void afterDestroy(EntryEvent event) {
   logCall("afterDestroy", event);
}

public void afterInvalidate(EntryEvent event) {
   logCall("afterInvalidate", event);
}

public void afterUpdate(EntryEvent event) {
   logCall("afterUpdate", event);
}

public void afterRegionDestroy(RegionEvent event) {
   logCall("afterRegionDestroy", event);
}

public void afterRegionInvalidate(RegionEvent event) {
   logCall("afterRegionInvalidate", event);
}

public void afterRegionLive(RegionEvent event) {
  logCall("afterRegionLive", event);
}

public void close() {
   logCall("close", null);
}
public void afterRegionClear(RegionEvent event) {
  logCall("afterRegionClear", event);

}
public void afterRegionCreate(RegionEvent event) {
  logCall("afterRegionCreate", event);

}


}
