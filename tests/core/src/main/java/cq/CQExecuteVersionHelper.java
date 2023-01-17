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
package cq;

import java.util.*;
import util.*;
import hydra.*;

import org.apache.geode.cache.query.*;

public class CQExecuteVersionHelper {
  public SelectResults executeWithInitialResults(CqQuery cq) throws CqClosedException, RegionNotFoundException, CqException {
    Log.getLogWriter().info("Calling executeWithInitializResults on " + cq);
    CqResults rs = cq.executeWithInitialResults();
    SelectResults sr = CQUtil.getSelectResults(rs);
    return sr;
  }
}
