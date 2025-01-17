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
package examples.dist;

//import util.*;
//import hydra.*;
import hydra.blackboard.Blackboard;
//import org.apache.geode.cache.*;

public class FlashcacheBB extends Blackboard {
   
// Blackboard variables
static String FLASHCACHE_BB_NAME = "FlashCache_Blackboard";
static String FLASHCACHE_BB_TYPE = "RMI";

// Blackboard counters
public static int NUM_GET_QUOTE;

public static FlashcacheBB bbInstance = null;

/**
 *  Get the FlashcacheBB
 */
public static FlashcacheBB getBB() {
   if (bbInstance == null) {
      synchronized ( FlashcacheBB.class ) {
         if (bbInstance == null) 
            bbInstance = new FlashcacheBB(FLASHCACHE_BB_NAME, FLASHCACHE_BB_TYPE);
      }
   }
   return bbInstance;
}
   
/**
 *  Zero-arg constructor for remote method invocations.
 */
public FlashcacheBB() {
}
   
/**
 *  Creates a sample blackboard using the specified name and transport type.
 */
public FlashcacheBB(String name, String type) {
   super(name, type, FlashcacheBB.class);
}
   
}
