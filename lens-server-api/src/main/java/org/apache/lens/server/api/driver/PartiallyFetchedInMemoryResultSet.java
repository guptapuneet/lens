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

package org.apache.lens.server.api.driver;

import java.util.ArrayList;
import java.util.List;

import javax.print.attribute.standard.NumberOfDocuments;

import org.apache.lens.api.query.ResultRow;
import org.apache.lens.server.api.error.LensException;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * This is a wrapper over InMemoryResultSet which pre-fetches requested number of rows in memory. All calls are
 * delegated to the underlying InMemoryResultSet except for the calls that access pre-fetched rows.
 *
 * This wrapper was created to support partial streaming of big result sets and complete streaming of SMALL result sets
 * along with persistence. The pre-fetched result available via {@link #getPreFetchedRows()} can be used for streaming
 * while the persistence logic can iterate over complete result set using {@link #hasNext()} and {@link #next()}.
 *
 * Please note that streaming and persistence can occur concurrently irrespective of the underlying InMemoryResultSet
 * implementation.
 */
@Slf4j
public class PartiallyFetchedInMemoryResultSet extends InMemoryResultSet {
  /**
   * Underlying in-memory result set
   */
  private InMemoryResultSet inMemoryRS;

  /**
   *Number for rows pre-fetched and kept in memory.
   */
  private int numOfPreFetchedRows;

  /**
   *Cursor for the pre-fetched in memory result.
   */
  private int cursor;

  /**
   * Indicates whether the underlying in-memory result has been completely pre-fetched and kept in memory.
   */
  @Getter
  private boolean isComplteleyFetched;

  /**
   * The pre-fteched in memory result cache.
   */
  @Getter
  private List<ResultRow> preFetchedRows;

  private long cacheValidUnitlTimeMillis;
  /**
   * Constructor
   * @param inMemoryRS : Underlying in-memory result set
   * @param reqPreFetchSize : requested number of rows to be pre-fetched and cached.
   * @throws LensException
   */
  public PartiallyFetchedInMemoryResultSet(InMemoryResultSet inMemoryRS, int reqPreFetchSize ,
      long cacheValidUnitlTimeMillis) throws LensException {
    this.inMemoryRS = inMemoryRS;
    this.cacheValidUnitlTimeMillis = cacheValidUnitlTimeMillis;
    if (reqPreFetchSize <= 0) {
      throw new IllegalArgumentException("Invalid pre fetch size " + reqPreFetchSize);
    }
    preFetchRows(reqPreFetchSize);
  }

  private void preFetchRows(int reqPreFetchSize) throws LensException {
    preFetchedRows = new ArrayList<ResultRow>(reqPreFetchSize + 1); //+1 as one extra row is read
    boolean hasNext = inMemoryRS.hasNext();
    while (hasNext) {
      if (numOfPreFetchedRows >= reqPreFetchSize) {
        break;
      }
      preFetchedRows.add(inMemoryRS.next());
      numOfPreFetchedRows++;
      hasNext = inMemoryRS.hasNext();
    }

    if (!hasNext) {
      isComplteleyFetched = true; // No more rows to be read form inMemory result.
    } else {
      isComplteleyFetched = false;
      //we have accessed ( hasNext() for ) one extra row. Lets cache it too.
      preFetchedRows.add(inMemoryRS.next());
      numOfPreFetchedRows++;
    }
    log.info("Pre-Fetched {} rows and isComplteleyFetched = {}", numOfPreFetchedRows, isComplteleyFetched);
  }

  @Override
  public boolean seekToStart() throws LensException {
    cursor = 0;
    if (!isComplteleyFetched && cursor > numOfPreFetchedRows ) {
      boolean seekedToStart = inMemoryRS.seekToStart();
      return seekedToStart;
    } else {
      return true;
    }
  }

  @Override
  public boolean hasNext() throws LensException {
    cursor++;
    if (cursor <= numOfPreFetchedRows) {
      return true;
    } else if (isComplteleyFetched) {
      return false;
    } else {
      return inMemoryRS.hasNext();
    }
  }

  @Override
  public ResultRow next() throws LensException {
    if (cursor <= numOfPreFetchedRows) {
      return preFetchedRows.get(cursor-1);
    } else {
      return inMemoryRS.next();
    }
  }

  @Override
  public void setFetchSize(int size) throws LensException {
    inMemoryRS.setFetchSize(size);
  }

  @Override
  public Integer size() throws LensException {
    if (isComplteleyFetched) {
      return numOfPreFetchedRows;
    } else {
      return inMemoryRS.size();
    }
  }

  @Override
  public LensResultSetMetadata getMetadata() throws LensException {
    return inMemoryRS.getMetadata();
  }

  @Override
  public boolean canBePurged() {
    if (isComplteleyFetched && System.currentTimeMillis() < this.cacheValidUnitlTimeMillis) {
      return false;
    } else {
      return inMemoryRS.canBePurged();
    }
  }

  @Override
  public void setFullyAccessed(boolean fullyAccessed) {
    inMemoryRS.setFullyAccessed(fullyAccessed);
  }
}
