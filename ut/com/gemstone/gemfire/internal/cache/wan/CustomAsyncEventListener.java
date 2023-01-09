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
package com.gemstone.gemfire.internal.cache.wan;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.gemstone.gemfire.cache.asyncqueue.AsyncEvent;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventListener;

public class CustomAsyncEventListener<K, V> implements AsyncEventListener<K, V> {

  private Map<Long, AsyncEvent<K, V>> eventsMap;
  private boolean exceptionThrown = false;

  public CustomAsyncEventListener() {
    this.eventsMap = new HashMap<Long, AsyncEvent<K, V>>();
  }

  public boolean processEvents(List<AsyncEvent<K, V>> events) {
    int i = 0;
    for (AsyncEvent event : events) {
      i++;
      if (!exceptionThrown && i == 40) {
        i = 0;
        exceptionThrown = true;
        throw new Error("TestError");
      }
      if (exceptionThrown) {
        eventsMap.put((Long) event.getKey(), event);
      }
    }
    return true;
  }

  public Map<Long, AsyncEvent<K, V>> getEventsMap() {
    return eventsMap;
  }

  public void close() {
  }
}