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
package com.examples.ds;

import org.apache.geode.cache.*;
import org.apache.geode.distributed.DistributedSystem;
import java.util.Date;
import java.util.Properties;

/**
 * Places various objects that use {@link DataSerializer}s and {@link
 * Instantiator}s into a cache {@link Region}.  Among other things,
 * this is used to test bug 31573.
 *
 * @since 3.5
 * @author David Whitlock
 */
public class PutDataSerializables {

  public static void main(String[] args) throws Throwable {
    Properties props = new Properties();
    DistributedSystem system = DistributedSystem.connect(props);
    Cache cache = CacheFactory.create(system);
    AttributesFactory factory = new AttributesFactory();
    Region region =
      cache.createRegion("DataSerializable",
                           factory.create());
    region.put("User", new User("Fred", 42));

    new CompanySerializer();
    Address address = new Address();
    Company company = new Company("My Company", address);

    region.put("Company", company);
    region.put("Employee",
               new Employee(43, "Bob", new Date(), company));

    Thread.sleep(60 * 1000);
  }

}
