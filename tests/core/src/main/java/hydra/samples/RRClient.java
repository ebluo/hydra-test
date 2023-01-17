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
   
package hydra.samples;

//import org.apache.geode.LogWriter;
import hydra.*;
//import java.io.*;
//import util.*;

/**
 *
 *  A sample client that executes round-robin tasks.
 *
 */

public class RRClient {

  public static void task1() {
    Log.getLogWriter().info("In task1");
  }
  public static void task2() {
    Log.getLogWriter().info("In task2");
  }
  public static void task3() {
    Log.getLogWriter().info("In task3");
  }
  public static void task4() {
    Log.getLogWriter().info("In task4");
  }
}
