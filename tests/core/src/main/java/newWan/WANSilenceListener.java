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
package newWan;

import util.SilenceListener;
import util.SilenceListenerBB;
import util.ValueHolder;

import org.apache.geode.cache.EntryEvent;

/**
 * WANSilenceListener can used for {@link WANTest#putSequentialKeysTask()} 
 * and for {@link WANTest#putSequentialKeysAndHATask()} tasks.
 * 
 * @author rdiyewar
 */

public class WANSilenceListener extends SilenceListener {
  public void afterUpdate(EntryEvent event) {
    SilenceListenerBB.getBB().getSharedCounters().setIfLarger(SilenceListenerBB.lastEventTime, System.currentTimeMillis());
    
    Long oldValue = (Long)((ValueHolder)event.getOldValue()).getMyValue();
    Long newValue = (Long)((ValueHolder)event.getNewValue()).getMyValue();
        
    if ((newValue.longValue() - oldValue.longValue()) > 1) {
      String err = new String("Difference between myValue in newValue and myValue in oldValue should not be more than 1, but found " + (newValue.longValue() - oldValue.longValue()) 
          + ". This is not expected as Gateway sender isBatchedConflationEnabled=false. " + toString("afterUpdate", event));
        WANBlackboard.throwException(err); 
    }
 }
}
