/*
 * Copyright 2016 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.linkedin.drelephant.analysis;

import com.linkedin.drelephant.ElephantContext;
import com.linkedin.drelephant.math.Statistics;
import controllers.MetricsController;
import models.AppResult;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import org.apache.hadoop.conf.Configuration;

import java.io.File;
import java.io.FileReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;


/**
 * This class provides a list of analysis promises to be generated under Hadoop YARN environment
 */
public class AnalyticJobReaderHadoop2 implements AnalyticJobGenerator {
  private static final Logger logger = Logger.getLogger(AnalyticJobReaderHadoop2.class);

  // We provide one minute job fetch delay due to the job sending lag from AM/NM to JobHistoryServer HDFS
  private static final long FETCH_DELAY = 60000;

  private long _lastTime = 0;
  private long _currentTime = 0;
  private final ObjectMapper _objectMapper = new ObjectMapper();

  private final Queue<AnalyticJob> _retryQueue = new ConcurrentLinkedQueue<AnalyticJob>();
  private final HashSet<String> filesVisited = new HashSet<String>();

  @Override
  public void configure(org.apache.hadoop.conf.Configuration configuration) throws IOException {
    return;
  }


  public void updateResourceManagerAddresses() {
    return;
  }

  /**
   *  Fetch all the succeeded and failed applications/analytic jobs from the resource manager.
   *
   * @return
   * @throws IOException
   * @throws AuthenticationException
   */
  @Override
  public List<AnalyticJob> fetchAnalyticJobs() throws IOException {
    List<AnalyticJob> appList = new ArrayList<AnalyticJob>();

    // There is a lag of job data from AM/NM to JobHistoryServer HDFS, we shouldn't use the current time, since there
    // might be new jobs arriving after we fetch jobs. We provide one minute delay to address this lag.
    _currentTime = System.currentTimeMillis() - FETCH_DELAY;

    logger.info("Fetching recent finished application runs between last time: " + (_lastTime + 1)
        + ", and current time: " + _currentTime);

    // Fetch all succeeded apps
    String succeededAppsURL = play.Configuration.root().getString("appslist.path");
    File folder = new File(succeededAppsURL);
    File[] listOfFiles = folder.listFiles();

    for (int i = 0; i < listOfFiles.length; i++) {
      String fileURL = listOfFiles[i].toString();
      if(!filesVisited.contains(fileURL)) {
        logger.info("The succeeded apps URL is " + fileURL);
        List<AnalyticJob> succeededApps = readApps(fileURL);
        appList.addAll(succeededApps);
        filesVisited.add(fileURL);
      }
    }

    return appList;
  }

  @Override
  public void addIntoRetries(AnalyticJob promise) {
    _retryQueue.add(promise);
    int retryQueueSize = _retryQueue.size();
    MetricsController.setRetryQueueSize(retryQueueSize);
    logger.info("Retry queue size is " + retryQueueSize);
  }

  /**
   * Connect to url using token and return the JsonNode
   *
   * @param str The JSON string to read
   * @return
   * @throws IOException Unable to get the stream
   */
  private JsonNode readJsonNode(String str) throws IOException {
    return _objectMapper.readTree(str);
  }

  /**
   * Parse the returned json from Resource manager
   *
   * @param url The REST call
   * @return
   * @throws IOException
   */
  private List<AnalyticJob> readApps(String url) throws IOException {
    List<AnalyticJob> appList = new ArrayList<AnalyticJob>();
    File file = new File(url);

    FileInputStream fis = new FileInputStream(file);
    BufferedReader reader = new BufferedReader(new InputStreamReader(fis));

    String line = reader.readLine();

    String jobId = "", appId = "", user = "", name = "", queueName = "", trackingUrl = "";
    long startTime = 0L, finishTime = 0L;
    while(line != null){
      //job_id,apptype,username,jobname,queuename,startTime,finishTime
      String[] result = line.split("\t");

      jobId = result[0];
      appId = jobId.replace("job", "application");
      user = result[2];
      name = result[3];
      queueName = result[4];
      startTime = Long.parseLong(result[5]);
      finishTime = Long.parseLong(result[6]);
      //trackingUrl = event.get("trackingUrl") != null ? event.get("trackingUrl").getValueAsText() : "";
      trackingUrl = "";

      ApplicationType type =
              ElephantContext.instance().getApplicationTypeForName(result[1]);

      // If the application type is supported
      if (type != null) {
        AnalyticJob analyticJob = new AnalyticJob();
        analyticJob.setAppId(appId).setAppType(type).setUser(user).setName(name).setQueueName(queueName)
                .setTrackingUrl(trackingUrl).setStartTime(startTime).setFinishTime(finishTime);

        logger.info("Adding the following to appList: " + analyticJob);
        appList.add(analyticJob);
      }

      line = reader.readLine();
    }

    return appList;
  }

}