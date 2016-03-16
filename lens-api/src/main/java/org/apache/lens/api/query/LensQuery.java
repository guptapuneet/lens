/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.api.query;


import org.apache.lens.api.LensConf;
import org.apache.lens.api.Priority;

 /**
  * Interface to provide all details about a query
  */
public interface LensQuery {

  /**
   * @return query handle
   */
  QueryHandle getQueryHandle();

  /**
   * @return query name
   */
  String getQueryName();

  /**
   * @return the query submitted by the user
   */
  String getUserQuery();

  /**
   * @return user who submitted the query
   */
  String getSubmittedUser();

  /**
   * @return Priority of the query
   */
  Priority getPriority();

  /**
   * @return true if query's result would be persisted by server.
   */
  boolean isPersistent();

  /**
   * @return driver which executed the query (Example: hive, jdbc, elastic-search, etc)
   */
  String getSelectedDriverName();

  /**
   * @return the final query (derived form user query) that was submitted by the driver for execution.
   */
  String getDriverQuery();

  /**
   * @return the status of this query.
   *
   * The {@link QueryStatus#getStatus()} method can be used to get the {@link QueryStatus.Status} enum that defines
   * the current state of the query. Also other utility methods are available to check the status of the query like
   * {@link QueryStatus#queued()}, {@link QueryStatus#successful()}, {@link QueryStatus#finished()},
   * {@link QueryStatus#failed()} and {@link QueryStatus#running()}
   */
  QueryStatus getStatus();

  /**
   * @return result path for this query if the query output was persisted by the server
   */
  String getResultSetPath();

  /**
   * @return operation handle associated with the driver, if any.
   */
  String getDriverOpHandle();

  /**
   * @return the conf that was used for executing this query
   */
  LensConf getQueryConf();

  /**
   * @return query submission time
   */
  long getSubmissionTime();

  /**
   * @return query launch time. This will be submission time + time spent by query waiting in the queue
   */
  long getLaunchTime();

  /**
   * @return the query execution start time on driver. This will >= launch time.
   */
  long getDriverStartTime();

  /**
   * @return the query execution end time on driver
   */
  long getDriverFinishTime();

  /**
   * @return the query finish time on server. This will be driver finish time + any extra time spent by server (like
   * formatting the result)
   */
  long getFinishTime();

  /**
   * @return the query close time when the query is purged by the server and no more operations are pending for it.
   * Note: not supported as of now.
   */
  long getClosedTime();

  /**
   * @return error code in case of query failures
   */
  Integer getErrorCode();

  /**
   * @return error message in case of query failures
   */
  String getErrorMessage();


  /**
   * @return the query handle that represents the query uniquely
   */
  String getQueryHandleString();
}
