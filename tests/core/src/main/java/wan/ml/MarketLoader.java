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
package wan.ml;
import org.apache.geode.SystemFailure;

import java.util.*;
import java.io.*;
import java.text.*;

// xml imports:
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.input.SAXBuilder;
import java.util.logging.*;

/**
 * Modified skeleton code -original skeleton code provided by Meryll Lynch
 * modified code provided by
 *
 * @author Owen Taylor 2005-09-30 this class needs
 *         -Djava.util.logging.config.file=<some-value>.properties in this file
 *         you should specify
 *         java.util.logging.FileHandler.formatter=com.ml.SuperSimpleFormatter
 *         otherwise the output is extremely verbose! This program must be
 *         started with the following System property: -Dvendor.classname=<fully-qualified-name>
 *         it will instantiate the subclass supplied by the vendor using
 *         Class.forName(System.getProperty("vendor.classname")).newInstance();
 *
 */

public abstract class MarketLoader {
  private SimpleDateFormat formatter = new SimpleDateFormat(
      "yyyy-MM-dd hh:mm:ss.SSS");

  public static MarketLoader loader;

  private static long start_time;

  private Element root_element;

  //default is first tick:
  protected static int START_TICK_ID=1;

  //default is one below the first tick:
  private static int currentTickId = 0;

  protected static String file_name = "MarketData.xml";

  // default of 5 /second:
  protected static int RATE_PER_SECOND = 5;

  // default is based on 1000 ms in a second (no latency)
  protected static long PURE_DELAY = 1000l;

  // default stop when file end is reached:
  protected static int END_ROW = Integer.MAX_VALUE;

  // default beginning of file:
  protected static int START_ROW = 0;

  protected static int TOTAL_QUOTES = 0;

  private static int CURRENT_ROW = 0;

  private SAXBuilder builder = new SAXBuilder();

  protected static Logger logger = Logger.getLogger("com.ml.MarketLoader");

  protected static FileHandler fh = null;

  protected Object completeNotification = new Object();

  /**
   * This program must be started with the following System property:<br />
   * -Dvendor.classname=<fully-qualified-name> <br />
   * it will instantiate the subclass supplied by the vendor using<br />
   * Class.forName(System.getProperty("vendor.classname")).newInstance();
   * <br />
   * this main method expects the following arguments:
   * <br />
   * -startid <some-value> <br />
   * the starting value for the market update tick ids
   * -startrow <some-value> <br />
   * the row of the file from which the loader instance starts submitting xml rows to the vendorPublishingAPI <br />
   * -endrow <some-value> <br />
   * the row of the file at which the loader instance stops submitting xml rows to the vendorPublishingAPI <br /><br />
   * The following settings:<br />
   * -startrow 100 -endrow 1000 <br />
   * result in the loader starting to submit data at row 100 and stopping the submission of data at row 1001 (row 1000 is submitted) <br />
   * -rate <some-value> <br />
   * rate / second measured of passing xml rows
   * to the vendorPublishingAPI <br />
   * filename the name of the file containing the xml data <br />
   * The first thing the main method does is call the vendorInit(Stringt [] args) method implemented by the vendor
   */
  public static void main(String args[]) {
    if (args.length < 1) {
      System.out
          .println("Did you know about all the args I can take?  they are: -startid -startrow -endrow -rate <filename>");
      System.out
          .println("Defaults are: 0 Integer.MAX_VALUE 5 Trades.xml");
      try {

        Thread.sleep(5000l);
      } 
      catch (VirtualMachineError e) {
        SystemFailure.initiateFailure(e);
        throw e;
      }
      catch (Throwable t) {
        // do nothing
      }
    }
    try {
      for (int x = 0; x < args.length; x++) {
        System.out.println("args[" + x + "] = " + args[x]);
      }
      System.out.println("logger = " + logger);
      fh = new FileHandler("MarketLoader_LOG.txt");
      loader = (MarketLoader) Class.forName(
          System.getProperty("vendor.classname")).newInstance();
      loader.vendorInit(args);
    } 
    catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    }
    catch (Throwable yt) {
      yt.printStackTrace();
      System.exit(1);
    }
    // Send logger output to our FileHandler.
    logger.addHandler(fh);
    // Request that every detail gets logged.
    logger.setLevel(Level.ALL);

    for (int arg = 0; arg < args.length; arg++) {
      if ("-rate".equals(args[arg])) {
        arg++;
        Integer i = new Integer(args[arg]);
        RATE_PER_SECOND = i.intValue();
      } else if ("-startrow".equals(args[arg])) {
        arg++;
        Integer i = new Integer(args[arg]);
        START_ROW = i.intValue();
      } else if ("-endrow".equals(args[arg])) {
        arg++;
        Integer i = new Integer(args[arg]);
        END_ROW = i.intValue();
      } else if ("-startid".equals(args[arg])){
        arg++;
        Integer i = new Integer(args[arg]);
        START_TICK_ID = i.intValue();
        //we iterate this value every time we write a trade:
        currentTickId = START_TICK_ID -1;
      } else {
        file_name = args[arg];
      }
    }
    loader.easyPumpMarketChanges();
  }

  /**
   * to be implemented by vendor: <br />an init method that allows the vendor to
   * prepare whatever is necessary to satisfy the requirements when the
   * vendorPublishingAPI method is called
   *
   */
  protected abstract void vendorInit(String[] args) throws Throwable;

  /**
   * to be implemented by vendor: <br />This method accepts the xml row containing a
   * single trade as a String and does what it needs to do to cache that trade
   * in the vendor's cache
   */
  protected abstract void vendorPublishingAPI(String xml) throws Throwable;

  /**
   * this method uses the various arguments to the main method to pump trades
   * through the vendorPublishingAPI <br />trades are read from a file and then row
   * by row fed through the vendorPublishingAPI
   *
   */
  protected void easyPumpMarketChanges() {

    String xml = null;
    boolean isPureDelayAdjustedForLatency = false;
    int trade_count = 0;
    start_time = System.currentTimeMillis();
    long lastTime = start_time;
    try {

      // Get the quotes into a collection
      FileReader reader = new FileReader(file_name);
      LineNumberReader line_reader = new LineNumberReader(reader);

      BufferedReader in = new BufferedReader(new LineNumberReader(new FileReader(file_name)));
      String line;
      List lines = new ArrayList();
      while ((line = in.readLine()) != null) {
        if ("<XMLROOT>".equals(line) || "</XMLROOT>".equals(line)) {
          continue;
        }
        lines.add(line);
      }
      in.close();

      String[] quotes = (String[]) lines.toArray(new String[0]);
      //System.out.println("Read " + quotes.length + " quotes");

      int currentQuoteIndex = 0;
      outside_loop: while (trade_count < TOTAL_QUOTES) {
        ArrayList batch = new ArrayList();

        for (int x = 0; x < RATE_PER_SECOND; x++) {
          // get a quote from the collection
          batch.add(quotes[currentQuoteIndex]);
          currentQuoteIndex++;
          if (currentQuoteIndex == quotes.length) {
            currentQuoteIndex = 0;
          }
        }

        //System.out.println("Sending batch of length " + batch.size());
        // deal with rate/second here:
        // it was agreed that we would pipe all requests at once and
        // sleep between the chunks
        // rather than try to sleep between each submission
        if (PURE_DELAY + lastTime > System.currentTimeMillis()) {
          Thread.sleep(PURE_DELAY + lastTime
              - System.currentTimeMillis());
        }

        Iterator i = batch.iterator();
        while (i.hasNext()) {
          xml = (String) i.next();
          // TODO: FIX THIS!
          if ( xml == null || xml.equals( "" ) ) {
            System.out.println( "MarketData XML String was null! Skipping . . . " );
            continue;
          }
          xml = assignTickId(xml);
          long writeStartNanoTime = System.currentTimeMillis();
          long writeWallStartTime = System.currentTimeMillis();
          vendorPublishingAPI(xml);
          bracketLog(writeWallStartTime, System.currentTimeMillis()
              - writeStartNanoTime, xml);
          trade_count++;
          if (trade_count % 100 == 0) {
            if (!(isPureDelayAdjustedForLatency)) {
              // we haven't measured the latency yet and our
              // PURE_DELAY is off due to that latency
              // set in motion the proper logic to measure latency
              long latency = (System.currentTimeMillis())
                  - (lastTime + PURE_DELAY);
              PURE_DELAY -= latency;
              isPureDelayAdjustedForLatency = true;
            }
            // System.out.println("Completed writing "+trade_count+"
            // trades");
          }
        }
        // group submitted to vendor - update the lastTime value:
        lastTime = System.currentTimeMillis();
      }
    } 
    catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    }
    catch (Throwable e) {
      e.printStackTrace();
    }

    long end_time = System.currentTimeMillis();

    System.out.println("Processed " + trade_count + " market updates in "
        + (end_time - start_time) + " ms.");
  }

  protected void waitForNotification() throws InterruptedException {
    synchronized (completeNotification) {
      completeNotification.wait();
    }
  }

  /**
   * NB: duration will be exposed as microseconds
   * <br />the output of this log will include:
    * <br />the time the write of this row was started, the tickId of this market update, and the duration the write took
   *
   */
  private void bracketLog(long writeStartTime, long duration, String xml) {
    Document doc = null;
    duration = duration / 1000l;
    try {
      Reader string_reader = new StringReader(xml);
      doc = builder.build(string_reader);
    } catch (Exception e) {
      System.out.println("Caught exception during XML parsing");
      e.printStackTrace();
    }
    root_element = doc.getRootElement();
    String id = getStringElement(root_element, "TickId");
    String timeStamp = formatter.format(new java.util.Date(writeStartTime));
    //logger.info(timeStamp + "," + id + "," + duration);
  }

  /**
  * assigns a new tickId to the current row of xml
  * ensuring we can track the proper number of ticks accross all loaders
  **/
  private static String assignTickId(String xml){
    //System.out.println ( "In assignTickID: XML is " + xml );
    currentTickId ++;
    StringBuffer builder = new StringBuffer(xml);
    int start = builder.indexOf("<TickId>");
    int end = builder.indexOf("</TickId>");
    builder.replace(start, end, "<TickId>"+currentTickId);
    return builder.toString();
  }

  private static String getStringElement(Element root_element, String name) {
    return (root_element.getChildText(name).trim());
  }
}
