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
package parReg.tx;

import java.util.*;
import java.io.Serializable;

import org.apache.geode.cache.*;

import util.*;
import hydra.*;

/** PartitionResolver for the parRegSerialView test.
 *  Entries are partitioned based on the integer portion of the key
 *  (Object_XXX) via the ViewRoutingObject.
 */
public class ViewTestResolver implements PartitionResolver, Serializable {

  public String getName() {
    return getClass().getName();
  }

  public Serializable getRoutingObject(EntryOperation opDetails) {
    ViewRoutingObject routingObject = new ViewRoutingObject(opDetails.getKey());
    return routingObject;
  }

  public void close() {}

}

