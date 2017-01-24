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

package org.apache.lens.cube.parse;

import static org.apache.lens.cube.metadata.DateFactory.*;
import static org.apache.lens.cube.metadata.DateUtil.*;
import static org.apache.lens.cube.parse.CandidateTablePruneCause.CandidateTablePruneCode.MISSING_PARTITIONS;
import static org.apache.lens.cube.parse.CubeTestSetup.*;
import static org.apache.lens.cube.parse.TestCubeRewriter.compareContains;
import static org.apache.lens.cube.parse.TestCubeRewriter.compareQueries;

import static org.apache.hadoop.hive.ql.parse.HiveParser.KW_AND;

import static org.testng.Assert.*;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lens.api.error.ErrorCollectionFactory;
import org.apache.lens.cube.error.LensCubeErrorCode;
import org.apache.lens.cube.error.NoCandidateFactAvailableException;
import org.apache.lens.cube.metadata.TimeRange;
import org.apache.lens.cube.metadata.UpdatePeriod;
import org.apache.lens.cube.parse.CandidateTablePruneCause.CandidateTablePruneCode;
import org.apache.lens.server.api.error.LensException;

import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.parse.ASTNode;

import org.antlr.runtime.CommonToken;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.google.common.base.Splitter;
import com.google.common.collect.Sets;
import lombok.Getter;

public class TestBaseCubeQueries extends TestQueryRewrite {
  @Getter
  private Configuration conf;
  private final String cubeName = CubeTestSetup.BASE_CUBE_NAME;

  @BeforeTest
  public void setupDriver() throws Exception {
    conf = new Configuration();
    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C1");
    conf.setBoolean(CubeQueryConfUtil.DISABLE_AUTO_JOINS, false);
    conf.setBoolean(CubeQueryConfUtil.ENABLE_SELECT_TO_GROUPBY, true);
    conf.setBoolean(CubeQueryConfUtil.ENABLE_GROUP_BY_TO_SELECT, true);
    conf.setBoolean(CubeQueryConfUtil.DISABLE_AGGREGATE_RESOLVER, false);
  }

  @Test
  public void testNoCandidateFactAvailableExceptionCompareTo() throws Exception {
    //maxCause : COLUMN_NOT_FOUND, Ordinal : 9
    NoCandidateFactAvailableException ne1 =(NoCandidateFactAvailableException)
            getLensExceptionInRewrite("select dim1, test_time_dim, msr3, msr13 from basecube where "
            + TWO_DAYS_RANGE, conf);
    //maxCause : FACT_NOT_AVAILABLE_IN_RANGE, Ordinal : 1
    NoCandidateFactAvailableException ne2 = (NoCandidateFactAvailableException)
            getLensExceptionInRewrite("select dim1 from " + cubeName + " where " + LAST_YEAR_RANGE, getConf());
    assertEquals(ne1.compareTo(ne2), 8);
  }

  @Test
  public void testColumnErrors() throws Exception {
    LensException e;

    e = getLensExceptionInRewrite("select msr11 + msr2 from basecube" + " where " + TWO_DAYS_RANGE, conf);
    e.buildLensErrorResponse(new ErrorCollectionFactory().createErrorCollection(), null, "testid");
    assertEquals(e.getErrorCode(),
      LensCubeErrorCode.NO_FACT_HAS_COLUMN.getLensErrorInfo().getErrorCode());
    assertTrue(e.getMessage().contains("msr11"), e.getMessage());
    assertTrue(e.getMessage().contains("msr2"), e.getMessage());
    // no fact has the all the dimensions queried
    e = getLensExceptionInRewrite("select dim1, test_time_dim, msr3, msr13 from basecube where "
      + TWO_DAYS_RANGE, conf);
    assertEquals(e.getErrorCode(),
        LensCubeErrorCode.NO_CANDIDATE_FACT_AVAILABLE.getLensErrorInfo().getErrorCode());
    NoCandidateFactAvailableException ne = (NoCandidateFactAvailableException) e;
    PruneCauses.BriefAndDetailedError pruneCauses = ne.getJsonMessage();
    String regexp = String.format(CandidateTablePruneCause.CandidateTablePruneCode.COLUMN_NOT_FOUND.errorFormat,
      "Column Sets: (.*?)", "queriable together");
    Matcher matcher = Pattern.compile(regexp).matcher(pruneCauses.getBrief());
    assertTrue(matcher.matches(), pruneCauses.getBrief());
    assertEquals(matcher.groupCount(), 1);
    String columnSetsStr = matcher.group(1);
    assertNotEquals(columnSetsStr.indexOf("test_time_dim"), -1, columnSetsStr);
    assertNotEquals(columnSetsStr.indexOf("msr3, msr13"), -1);

    /**
     * Verifying the BriefAndDetailedError:
     * 1. Check for missing columns(COLUMN_NOT_FOUND)
     *    and check the respective tables for each COLUMN_NOT_FOUND
     * 2. check for ELEMENT_IN_SET_PRUNED
     *
     */
    boolean columnNotFound = false;
    List<String> testTimeDimFactTables = Arrays.asList("testfact3_base", "testfact1_raw_base", "testfact3_raw_base",
      "testfact5_base", "testfact6_base", "testfact4_raw_base");
    List<String> factTablesForMeasures = Arrays.asList("testfact_deprecated", "testfact2_raw_base", "testfact2_base",
            "testfact5_raw_base");
    for (Map.Entry<String, List<CandidateTablePruneCause>> entry : pruneCauses.getDetails().entrySet()) {
      if (entry.getValue().contains(CandidateTablePruneCause.columnNotFound("test_time_dim"))) {
        columnNotFound = true;
        compareStrings(testTimeDimFactTables, entry);
      }
      if (entry.getValue().contains(CandidateTablePruneCause.columnNotFound("msr3", "msr13"))) {
        columnNotFound = true;
        compareStrings(factTablesForMeasures, entry);
      }
    }
    Assert.assertTrue(columnNotFound);
    assertEquals(pruneCauses.getDetails().get("testfact1_base"),
      Arrays.asList(new CandidateTablePruneCause(CandidateTablePruneCode.ELEMENT_IN_SET_PRUNED)));
  }

  private void compareStrings(List<String> factTablesList, Map.Entry<String, List<CandidateTablePruneCause>> entry) {
    String factTablesString = entry.getKey();
    Iterable<String> factTablesIterator = Splitter.on(',').split(factTablesString);
    for (String factTable : factTablesIterator) {
      Assert.assertTrue(factTablesList.contains(factTable), "Not selecting" + factTable + "fact table");
    }
  }

  @Test
  public void testCommonDimensions() throws Exception {
    String hqlQuery = rewrite("select dim1, SUM(msr1) from basecube" + " where " + TWO_DAYS_RANGE, conf);
    String expected =
      getExpectedQuery(cubeName, "SELECT (basecube.dim1) as `dim1`, sum((basecube.msr1)) as `sum(msr1)` FROM ",
          null, " group by basecube.dim1",
        getWhereForHourly2days(cubeName, "C1_testfact1_raw_base"));
    compareQueries(hqlQuery, expected);

    hqlQuery = rewrite("select dim1, SUM(msr1), msr2 from basecube" + " where " + TWO_DAYS_RANGE, conf);
    expected =
      getExpectedQuery(cubeName, "SELECT (basecube.dim1) as `dim1`, sum((basecube.msr1)) as `sum(msr1)`, "
          + "(basecube.msr2) as `msr2` FROM ", null, " group by basecube.dim1",
          getWhereForHourly2days(cubeName, "C1_testfact1_raw_base"));
    compareQueries(hqlQuery, expected);

    hqlQuery = rewrite("select dim1, roundedmsr2 from basecube" + " where " + TWO_DAYS_RANGE, conf);
    expected =
      getExpectedQuery(cubeName, "SELECT (basecube.dim1) as `dim1`, round((sum((basecube.msr2)) / 1000)) "
          + "as `roundedmsr2` FROM ", null, " group by basecube.dim1",
          getWhereForDailyAndHourly2days(cubeName, "C1_testFact1_BASE"));
    compareQueries(hqlQuery, expected);

    hqlQuery =
      rewrite("select booleancut, msr2 from basecube" + " where " + TWO_DAYS_RANGE + " and substrexpr != 'XYZ'", conf);
    expected =
      getExpectedQuery(cubeName, "SELECT (((basecube.dim1) != 'x') and ((basecube.dim2) != 10)) as `booleancut`, "
          + "sum((basecube.msr2)) as `msr2` FROM",
          null, " and substr(basecube.dim1, 3) != 'XYZ' "
          + "group by basecube.dim1 != 'x' AND basecube.dim2 != 10",
        getWhereForHourly2days(cubeName, "C1_testfact1_raw_base"));
    compareQueries(hqlQuery, expected);

    hqlQuery = rewrite("select dim1, msr12 from basecube" + " where " + TWO_DAYS_RANGE, conf);
    expected =
      getExpectedQuery(cubeName, "SELECT (basecube.dim1) as `dim1`, sum((basecube.msr12)) as `msr12` FROM ", null,
          " group by basecube.dim1", getWhereForDailyAndHourly2days(cubeName, "C1_testFact2_BASE"));
    compareQueries(hqlQuery, expected);
  }
  // TODO union : Fix after CandidateFact deleted
  /*
  @Test
  public void testMultiFactQueryWithNoDimensionsSelected() throws Exception {
    CubeQueryContext ctx = rewriteCtx("select roundedmsr2, msr12 from basecube" + " where " + TWO_DAYS_RANGE, conf);
    Set<String> candidateFacts = new HashSet<String>();
    for (CandidateFact cfact : ctx.getCandidateFacts()) {
      candidateFacts.add(cfact.getName().toLowerCase());
    }
    Assert.assertTrue(candidateFacts.contains("testfact1_base"));
    Assert.assertTrue(candidateFacts.contains("testfact2_base"));
    String hqlQuery = ctx.toHQL();
    String expected1 =
      getExpectedQuery(cubeName, "select sum(basecube.msr12) as `msr12` FROM ", null,
        null, getWhereForDailyAndHourly2days(cubeName, "C1_testFact2_BASE"));
    String expected2 =
      getExpectedQuery(cubeName, "select round(sum(basecube.msr2)/1000) as `roundedmsr2` FROM ", null,
        null, getWhereForDailyAndHourly2days(cubeName, "C1_testFact1_BASE"));
    compareContains(expected1, hqlQuery);
    compareContains(expected2, hqlQuery);
    String lower = hqlQuery.toLowerCase();
    assertTrue(lower.startsWith("select mq2.roundedmsr2 roundedmsr2, mq1.msr12 msr12 from ")
      || lower.startsWith("select mq1.roundedmsr2 roundedmsr2, mq2.msr12 msr12 from "), hqlQuery);
    assertTrue(lower.contains("mq1 full outer join") && lower.endsWith("mq2"), hqlQuery);
    assertFalse(lower.contains("mq2 on"), hqlQuery);
    assertFalse(lower.contains("<=>"), hqlQuery);
  }
*/

  // TODO union : Fix after CandidateFact deleted
  /*
  @Test
  public void testMoreThanTwoFactQueryWithNoDimensionsSelected() throws Exception {
    CubeQueryContext ctx = rewriteCtx("select roundedmsr2, msr14, msr12 from basecube" + " where " + TWO_DAYS_RANGE,
      conf);
    Set<String> candidateFacts = new HashSet<String>();
    for (CandidateFact cfact : ctx.getCandidateFacts()) {
      candidateFacts.add(cfact.getName().toLowerCase());
    }
    Assert.assertEquals(candidateFacts.size(), 3);
    Assert.assertTrue(candidateFacts.contains("testfact1_base"));
    Assert.assertTrue(candidateFacts.contains("testfact2_base"));
    Assert.assertTrue(candidateFacts.contains("testfact3_base"));
    String hqlQuery = ctx.toHQL();
    String expected1 = getExpectedQuery(cubeName, "select sum(basecube.msr12) as `msr12` FROM ", null, null,
      getWhereForDailyAndHourly2days(cubeName, "C1_testFact2_BASE"));
    String expected2 = getExpectedQuery(cubeName, "select round(sum(basecube.msr2)/1000) as `roundedmsr2` FROM ", null,
      null, getWhereForDailyAndHourly2days(cubeName, "C1_testFact1_BASE"));
    String expected3 = getExpectedQuery(cubeName, "select count((basecube.msr14)) as `msr14` FROM ", null, null,
      getWhereForDailyAndHourly2days(cubeName, "C1_testFact3_BASE"));
    compareContains(expected1, hqlQuery);
    compareContains(expected2, hqlQuery);
    compareContains(expected3, hqlQuery);
    String lower = hqlQuery.toLowerCase();
    assertTrue(lower.startsWith("select mq1.roundedmsr2 roundedmsr2, mq3.msr14 msr14, mq2.msr12 msr12 from ") || lower
      .startsWith("select mq3.roundedmsr2 roundedmsr2, mq1.msr14 msr14, mq2.msr12 msr12 from ") || lower
      .startsWith("select mq2.roundedmsr2 roundedmsr2, mq3.msr14 msr14, mq1.msr12 msr12 from ") || lower
      .startsWith("select mq3.roundedmsr2 roundedmsr2, mq2.msr14 msr14, mq1.msr12 msr12 from ") || lower
      .startsWith("select mq1.roundedmsr2 roundedmsr2, mq2.msr14 msr14, mq3.msr12 msr12 from ") || lower
      .startsWith("select mq2.roundedmsr2 roundedmsr2, mq1.msr14 msr14, mq3.msr12 msr12 from "), hqlQuery);
    assertTrue(lower.contains("mq1 full outer join") && lower.endsWith("mq3"));
    assertFalse(lower.contains("mq3 on"), hqlQuery);
    assertFalse(lower.contains("mq2 on"), hqlQuery);
    assertFalse(lower.contains("<=>"), hqlQuery);
  }
*/
  @Test
  public void testMultiFactQueryWithSingleCommonDimension() throws Exception {
    String hqlQuery = rewrite("select dim1, roundedmsr2, msr12 from basecube" + " where " + TWO_DAYS_RANGE, conf);
    String expected1 =
      getExpectedQuery(cubeName, "SELECT (basecube.dim1) as `alias0`, sum((basecube.msr2)) as `alias1`, "
          + "0.0 as `alias2` FROM ", null, " group by basecube.dim1",
          getWhereForDailyAndHourly2days(cubeName, "C1_testFact1_BASE"));
    String expected2 = getExpectedQuery(cubeName, "SELECT (basecube.dim1) as `alias0`, 0.0 as `alias1`, "
        + "sum((basecube.msr12)) as `alias2` FROM ", null, " group by basecube.dim1",
        getWhereForDailyAndHourly2days(cubeName, "C1_testFact2_BASE"));
    compareContains(expected1, hqlQuery);
    compareContains(expected2, hqlQuery);
    String lower = hqlQuery.toLowerCase();
    assertTrue(
      lower.startsWith("select (basecube.alias0) as `dim1`, round((sum((basecube.alias1)) / 1000)) as `roundedmsr2`, "
          + "sum((basecube.alias2)) as `msr12` from"), hqlQuery);
    assertTrue(hqlQuery.contains("UNION ALL") && hqlQuery.endsWith("GROUP BY (basecube.alias0)"),
      hqlQuery);
  }

  @Test
  public void testMultiFactQueryWithSingleCommonDimensionWithLightestFactFirst() throws Exception {
    Configuration tConf = new Configuration(conf);
    tConf.setBoolean(CubeQueryConfUtil.LIGHTEST_FACT_FIRST, true);
    String hqlQuery = rewrite("select dim1, roundedmsr2, msr12 from basecube" + " where " + TWO_DAYS_RANGE, tConf);
    String expected1 = getExpectedQuery(cubeName, "SELECT (basecube.dim1) as `alias0`, 0.0 as `alias1`, " +
          "sum((basecube.msr12)) as `alias2` FROM ", null, " group by basecube.dim1",
        getWhereForDailyAndHourly2days(cubeName, "C1_testFact2_BASE"));
    String expected2 = getExpectedQuery(cubeName, "SELECT (basecube.dim1) as `alias0`, sum((basecube.msr2)) "
        + "as `alias1`, 0.0 as `alias2` FROM ", null, " group by basecube.dim1",
        getWhereForDailyAndHourly2days(cubeName, "C1_testFact1_BASE"));
    compareContains(expected1, hqlQuery);
    compareContains(expected2, hqlQuery);
    String lower = hqlQuery.toLowerCase();
    assertTrue(lower.startsWith("select (basecube.alias0) as `dim1`, round((sum((basecube.alias1)) / 1000)) " +
        "as `roundedmsr2`, sum((basecube.alias2)) as `msr12` from"), hqlQuery);
    assertTrue(hqlQuery.contains("UNION ALL") && hqlQuery.endsWith("GROUP BY (basecube.alias0)"),
      hqlQuery);
  }

  @Test
  public void testMultiFactQueryWithExpressionsFromMultipleFacts() throws Exception {
    Configuration tConf = new Configuration(conf);
    tConf.setBoolean(CubeQueryConfUtil.LIGHTEST_FACT_FIRST, true);
    String hqlQuery = rewrite("select  dim1, roundedmsr2, flooredmsr12 from basecube" + " where "
            + TWO_DAYS_RANGE, tConf);
    String expected1 = getExpectedQuery(cubeName, "SELECT (basecube.dim1) as `alias0`, 0.0 as `alias1`, "
        + "sum((basecube.msr12)) as `alias2` FROM ", null, " group by basecube.dim1",
        getWhereForDailyAndHourly2days(cubeName, "C1_testFact2_BASE"));
    String expected2 = getExpectedQuery(cubeName, "select basecube.dim1 as `dim1`, round(sum(basecube.msr2)/1000) "
        + "as `roundedmsr2` FROM ", null, " group by basecube.dim1",
        getWhereForDailyAndHourly2days(cubeName, "C1_testFact1_BASE"));
    compareContains(expected1, hqlQuery);
    compareContains(expected2, hqlQuery);
    String lower = hqlQuery.toLowerCase();
    assertTrue(lower.startsWith("select (basecube.alias0) as `dim1`, round((sum((basecube.alias1)) / 1000)) "
        + "as `roundedmsr2`, floor(sum((basecube.alias2))) as `flooredmsr12` from"), hqlQuery);
    assertTrue(hqlQuery.contains("UNION ALL") && hqlQuery.endsWith("GROUP BY (basecube.alias0)"),
        hqlQuery);
  }

  @Test
  public void testMultiFactQueryWithSingleCommonDimensionWithColumnsSwapped() throws Exception {
    // columns in select interchanged
    String hqlQuery = rewrite("select dim1, msr12, roundedmsr2 from basecube" + " where " + TWO_DAYS_RANGE, conf);
    String expected1 =
      getExpectedQuery(cubeName, "SELECT (basecube.dim1) as `alias0`, sum((basecube.msr12)) as `alias1`, "
          + "0.0 as `alias2` FROM", null, " group by basecube.dim1",
          getWhereForDailyAndHourly2days(cubeName, "C1_testFact2_BASE"));
    String expected2 = getExpectedQuery(cubeName,
        "SELECT (basecube.dim1) as `alias0`, 0.0 as `alias1`, sum((basecube.msr2)) as `alias2` FROM ", null,
        " group by basecube.dim1", getWhereForDailyAndHourly2days(cubeName, "C1_testFact1_BASE"));
    compareContains(expected1, hqlQuery);
    compareContains(expected2, hqlQuery);
    String lower = hqlQuery.toLowerCase();
    assertTrue(
      lower.startsWith("select (basecube.alias0) as `dim1`, sum((basecube.alias1)) as `msr12`, "
          + "round((sum((basecube.alias2)) / 1000)) as `roundedmsr2` from"), hqlQuery);

    assertTrue(hqlQuery.contains("UNION ALL") && hqlQuery.endsWith("GROUP BY (basecube.alias0)"), hqlQuery);
  }

  @Test
  public void testMultiFactQueryInvolvingThreeFactTables() throws Exception {
    // query with 3 fact tables
    String hqlQuery = rewrite("select dim1, d_time, msr12, roundedmsr2, msr13, msr3 from basecube where "
        + TWO_DAYS_RANGE, conf);
    String expected1 =
      getExpectedQuery(cubeName, " SELECT (basecube.dim1) as `alias0`, (basecube.d_time) as `alias1`, "
          + "sum((basecube.msr12)) as `alias2`, 0.0 as `alias3`, 0.0 as `alias4`, 0.0 as `alias5` FROM ",
          null, " group by basecube.dim1, (basecube.d_time)",
          getWhereForDailyAndHourly2days(cubeName, "C1_testFact2_BASE"));
    String expected2 = getExpectedQuery(
        cubeName,
        "SELECT (basecube.dim1) as `alias0`, (basecube.d_time) as `alias1`, 0.0 as `alias2`, "
            + "sum((basecube.msr2)) as `alias3`, 0.0 as `alias4`, max((basecube.msr3)) as `alias5` FROM ", null,
        " group by basecube.dim1, (basecube.d_time)", getWhereForDailyAndHourly2days(cubeName, "C1_testFact1_BASE"));
    String expected3 =
      getExpectedQuery(cubeName, "SELECT (basecube.dim1) as `alias0`, (basecube.d_time) as `alias1`, 0.0 as `alias2`, "
          + "0.0 as `alias3`, max((basecube.msr13)) as `alias4`, 0.0 as `alias5` FROM ", null,
        " group by basecube.dim1, (basecube.d_time)", getWhereForDailyAndHourly2days(cubeName, "c1_testfact3_base"));
    compareContains(expected1, hqlQuery);
    compareContains(expected2, hqlQuery);
    compareContains(expected3, hqlQuery);
    assertTrue(
      hqlQuery.toLowerCase().startsWith(
        "select (basecube.alias0) as `dim1`, (basecube.alias1) as `d_time`, sum((basecube.alias2)) as `msr12`, "
            + "round((sum((basecube.alias3)) / 1000)) as `roundedmsr2`, max((basecube.alias4)) as `msr13`, "
            + "max((basecube.alias5)) as `msr3` from "), hqlQuery);
    assertTrue(hqlQuery.contains("UNION ALL") && hqlQuery.endsWith("GROUP BY (basecube.alias0), (basecube.alias1)"),
        hqlQuery);
  }

  @Test
  public void testMultiFactQueryWithTwoCommonDimensions() throws Exception {
    // query two dim attributes
    String hqlQuery = rewrite("select dim1, dim11, msr12, roundedmsr2 from basecube where " + TWO_DAYS_RANGE, conf);
    String expected1 = getExpectedQuery(cubeName, "SELECT (basecube.dim1) as `alias0`, (basecube.dim11) as `alias1`, "
        + "sum((basecube.msr12)) as `alias2`, 0.0 as `alias3` FROM ", null, " group by basecube.dim1",
        getWhereForDailyAndHourly2days(cubeName, "C1_testFact2_BASE"));
    String expected2 = getExpectedQuery(cubeName, "SELECT (basecube.dim1) as `alias0`, (basecube.dim11) as `alias1`, "
        + "0.0 as `alias2`, sum((basecube.msr2)) as `alias3` FROM ", null, " group by basecube.dim1",
        getWhereForDailyAndHourly2days(cubeName, "C1_testFact1_BASE"));
    compareContains(expected1, hqlQuery);
    compareContains(expected2, hqlQuery);
    assertTrue(hqlQuery.toLowerCase().startsWith(
      "select (basecube.alias0) as `dim1`, (basecube.alias1) as `dim11`, sum((basecube.alias2)) as `msr12`, "
          + "round((sum((basecube.alias3)) / 1000)) as `roundedmsr2` from"), hqlQuery);

    assertTrue(hqlQuery.contains("UNION ALL")
      && hqlQuery.endsWith("GROUP BY (basecube.alias0), (basecube.alias1)"), hqlQuery);
  }

  @Test
  public void testMultiFactQueryWithNoAggregates() throws Exception {
    // no aggregates in the query
    String hqlQuery = rewrite("select dim1, msr11, roundedmsr2 from basecube where " + TWO_DAYS_RANGE, conf);
    String expected1 =
      getExpectedQuery(cubeName, "SELECT (basecube.dim1) as `alias0`, (basecube.msr11) as `alias1`, "
          + "0.0 as `alias2` FROM ", null, null, getWhereForHourly2days(cubeName, "C1_testfact2_raw_base"));
    String expected2 = getExpectedQuery(cubeName,
        "SELECT (basecube.dim1) as `alias0`, 0.0 as `alias1`, round(((basecube.msr2) / 1000)) "
            + "as `alias2` FROM ", null, null,
        getWhereForHourly2days(cubeName, "C1_testfact1_raw_base"));
    compareContains(expected1, hqlQuery);
    compareContains(expected2, hqlQuery);
    assertTrue(hqlQuery.toLowerCase().startsWith(
      "select (basecube.alias0) as `dim1`, (basecube.alias1) as `msr11`, " +
          "(basecube.alias2) as `roundedmsr2` from"), hqlQuery);
    assertTrue(hqlQuery.contains("UNION ALL") && hqlQuery.endsWith("as basecube"),
      hqlQuery);
  }

  @Test
  public void testMultiFactQueryWithColumnAliases() throws Exception {
    // query with aliases passed
    String hqlQuery =
      rewrite("select dim1 d1, msr12 `my msr12`, roundedmsr2 m2 from basecube where " + TWO_DAYS_RANGE, conf);
    String expected1 =
      getExpectedQuery(cubeName, "SELECT (basecube.dim1) as `alias0`, sum((basecube.msr12)) as `alias1`, "
          + "0.0 as `alias2` FROM ", null, " group by basecube.dim1",
          getWhereForDailyAndHourly2days(cubeName, "C1_testFact2_BASE"));
    String expected2 =
      getExpectedQuery(cubeName, "SELECT (basecube.dim1) as `alias0`, 0.0 as `alias1`, sum((basecube.msr2)) "
          + "as `alias2` FROM ", null, " group by basecube.dim1",
          getWhereForDailyAndHourly2days(cubeName, "C1_testFact1_BASE"));
    compareContains(expected1, hqlQuery);
    compareContains(expected2, hqlQuery);
    assertTrue(hqlQuery.toLowerCase().startsWith(
      "select (basecube.alias0) as `d1`, sum((basecube.alias1)) as `my msr12`, round((sum((basecube.alias2)) / 1000)) as `m2` from"),
        hqlQuery);
    assertTrue(hqlQuery.contains("UNION ALL") && hqlQuery.endsWith("GROUP BY (basecube.alias0)"),
      hqlQuery);
  }

  @Test
  public void testMultiFactQueryWithColumnAliasesAsFunctions() throws Exception {
    String hqlQuery =
      rewrite("select dim1 d1, msr12 `sum(msr12)`, roundedmsr2 as `round(sum(msr2)/1000)` from basecube where "
        + TWO_DAYS_RANGE, conf);
    String expected1 =
      getExpectedQuery(cubeName, "SELECT (basecube.dim1) as `alias0`, sum((basecube.msr12)) as `alias1`, "
          + "0.0 as `alias2` FROM ", null, " group by basecube.dim1",
          getWhereForDailyAndHourly2days(cubeName, "C1_testFact2_BASE"));
    String expected2 =
      getExpectedQuery(cubeName, "SELECT (basecube.dim1) as `alias0`, 0.0 as `alias1`, sum((basecube.msr2)) "
          + "as `alias2` FROM ", null, " group by basecube.dim1",
          getWhereForDailyAndHourly2days(cubeName, "C1_testFact1_BASE"));
    compareContains(expected1, hqlQuery);
    compareContains(expected2, hqlQuery);
    assertTrue(hqlQuery.toLowerCase().startsWith(
        "select (basecube.alias0) as `d1`, sum((basecube.alias1)) as `sum(msr12)`, round((sum((basecube.alias2)) / 1000)) " +
            "as `round(sum(msr2)/1000)` from"), hqlQuery);
    assertTrue(hqlQuery.contains("UNION ALL") && hqlQuery.endsWith("GROUP BY (basecube.alias0)"),
      hqlQuery);
  }

  @Test
  public void testMultiFactQueryWithAliasAsColumnName() throws Exception {
    String hqlQuery =
      rewrite("select dim1 d1, msr12 `my msr12`, roundedmsr2 as `msr2` from basecube where " + TWO_DAYS_RANGE, conf);

    String expected1 =
      getExpectedQuery(cubeName, "SELECT (basecube.dim1) as `alias0`, sum((basecube.msr12)) as `alias1`, "
          + "0.0 as `alias2` FROM ", null, " group by basecube.dim1",
          getWhereForDailyAndHourly2days(cubeName, "C1_testFact2_BASE"));
    String expected2 =
      getExpectedQuery(cubeName, "SELECT (basecube.dim1) as `alias0`, 0.0 as `alias1`, sum((basecube.msr2)) " +
          "as `alias2` FROM ", null, " group by basecube.dim1",
          getWhereForDailyAndHourly2days(cubeName, "C1_testFact1_BASE"));
    compareContains(expected1, hqlQuery);
    compareContains(expected2, hqlQuery);
    assertTrue(hqlQuery.toLowerCase().startsWith(
        "select (basecube.alias0) as `d1`, sum((basecube.alias1)) as `my msr12`, round((sum((basecube.alias2)) / 1000)) " +
            "as `msr2` from"), hqlQuery);
    assertTrue(hqlQuery.contains("UNION ALL") && hqlQuery.endsWith("(basecube.alias0)"),
      hqlQuery);
  }

  @Test
  public void testMultiFactQueryWithAliasAsExpressionName() throws Exception {
    String hqlQuery =
      rewrite("select dim1 d1, msr12 `my msr12`, roundedmsr2 as `roundedmsr2` from basecube where " + TWO_DAYS_RANGE,
        conf);

    String expected1 =
      getExpectedQuery(cubeName, "SELECT (basecube.dim1) as `alias0`, sum((basecube.msr12)) as `alias1`, "
          + "0.0 as `alias2` FROM", null, " group by basecube.dim1",
          getWhereForDailyAndHourly2days(cubeName, "C1_testFact2_BASE"));
    String expected2 =
      getExpectedQuery(cubeName, "SELECT (basecube.dim1) as `alias0`, 0.0 as `alias1`, sum((basecube.msr2)) " +
          "as `alias2` FROM", null,
          " group by basecube.dim1", getWhereForDailyAndHourly2days(cubeName, "C1_testFact1_BASE"));
    compareContains(expected1, hqlQuery);
    compareContains(expected2, hqlQuery);
    assertTrue(hqlQuery.toLowerCase().startsWith(
        "select (basecube.alias0) as `d1`, sum((basecube.alias1)) as `my msr12`, round((sum((basecube.alias2)) / 1000)) " +
            "as `roundedmsr2` from"), hqlQuery);
    assertTrue(hqlQuery.contains("UNION ALL") && hqlQuery.endsWith("GROUP BY (basecube.alias0)"),
      hqlQuery);
  }

  @Test
  public void testMultiFactQueryWithExprOnDimsWithoutAliases() throws Exception {
    String hqlQuery =
      rewrite("select reverse(dim1), ltrim(dim1), msr12, roundedmsr2 from basecube where " + TWO_DAYS_RANGE, conf);
    String expected1 =
      getExpectedQuery(cubeName, "SELECT reverse((basecube.dim1)) as `alias0`, ltrim((basecube.dim1)) as `alias1`, "
          + "sum((basecube.msr12)) as `alias2`, 0.0 as `alias3` FROM ", null,
          " group by reverse(basecube.dim1), ltrim(basecube.dim1)",
        getWhereForDailyAndHourly2days(cubeName, "C1_testFact2_BASE"));
    String expected2 =
      getExpectedQuery(cubeName, "SELECT reverse((basecube.dim1)) as `alias0`, ltrim((basecube.dim1)) as `alias1`, "
          + "0.0 as `alias2`, sum((basecube.msr2)) as `alias3` FROM ", null,
        " group by reverse(basecube.dim1), ltrim(basecube.dim1)",
        getWhereForDailyAndHourly2days(cubeName, "C1_testFact1_BASE"));
    compareContains(expected1, hqlQuery);
    compareContains(expected2, hqlQuery);
    assertTrue(hqlQuery.toLowerCase().startsWith("select (basecube.alias0) as `reverse(dim1)`, (basecube.alias1) "
        + "as `ltrim(dim1)`, sum((basecube.alias2)) as `msr12`, round((sum((basecube.alias3)) / 1000)) as `roundedmsr2` from"),
      hqlQuery);
    assertTrue(hqlQuery.contains("UNION ALL")
      && hqlQuery.endsWith("GROUP BY (basecube.alias0), (basecube.alias1)"), hqlQuery);
  }

  @Test
  public void testMultiFactQueryWithDirectMsr() throws Exception {
    String hqlQuery =
      rewrite("select reverse(dim1), directMsrExpr as directMsr, roundedmsr2 from basecube where " + TWO_DAYS_RANGE,
        conf);
    String expected1 =
      getExpectedQuery(cubeName, "select reverse(basecube.dim1) as `expr1`, "
        + "max(basecube.msr13) + count(basecube . msr14) as `expr2` FROM ", null,
        " group by reverse(basecube.dim1)", getWhereForDailyAndHourly2days(cubeName, "C1_testFact3_BASE"));
    String expected2 =
      getExpectedQuery(cubeName, "select reverse(basecube.dim1) as expr1, "
        + "round(sum(basecube.msr2)/1000) as `roundedmsr2` FROM ", null, " group by reverse(basecube.dim1)",
        getWhereForDailyAndHourly2days(cubeName, "C1_testFact1_BASE"));
    compareContains(expected1, hqlQuery);
    compareContains(expected2, hqlQuery);
    assertTrue(hqlQuery.toLowerCase().startsWith(
      "select coalesce(mq1.expr1, mq2.expr1) `reverse(dim1)`, mq2.expr2 `directmsr`, mq1.roundedmsr2 roundedmsr2 "
        + "from ")
      || hqlQuery.toLowerCase().startsWith(
        "select coalesce(mq1.expr1, mq2.expr1) `reverse(dim1)`, mq1.expr2 `directmsr`, mq2.roundedmsr2 roundedmsr2 "
          + "from "),
      hqlQuery.toLowerCase());
    assertTrue(hqlQuery.contains("mq1 full outer join ") && hqlQuery.endsWith("mq2 on mq1.expr1 <=> mq2.expr1"),
      hqlQuery);
  }

  @Test
  public void testMultiFactQueryWithNoDefaultAggregates() throws Exception {
   // query with non default aggregate
    String hqlQuery = rewrite("select dim1, avg(msr12), avg(msr2) from basecube where " + TWO_DAYS_RANGE, conf);
    String expected1 =
      getExpectedQuery(cubeName, "SELECT (basecube.dim1) as `alias0`, avg((basecube.msr12)) as `alias1`,"
          + " 0.0 as `alias2` FROM  ", null, " group by basecube.dim1",
          getWhereForHourly2days(cubeName, "C1_testfact2_raw_base"));
    String expected2 =
      getExpectedQuery(cubeName, "SELECT (basecube.dim1) as `alias0`, 0.0 as `alias1`, avg((basecube.msr2)) " +
          "as `alias2` FROM ", null, " group by basecube.dim1",
          getWhereForHourly2days(cubeName, "C1_testfact1_raw_base"));
    compareContains(expected1, hqlQuery);
    compareContains(expected2, hqlQuery);
    assertTrue(hqlQuery.toLowerCase().startsWith(
      "select (basecube.alias0) as `dim1`, avg((basecube.alias1)) as `avg(msr12)`, avg((basecube.alias2)) "
          + "as `avg(msr2)` from"), hqlQuery);
    assertTrue(hqlQuery.contains("UNION ALL") && hqlQuery.endsWith("GROUP BY (basecube.alias0)"), hqlQuery);
  }

  @Test
  public void testMultiFactQueryWithJoins() throws Exception {
    // query with join
    String hqlQuery = rewrite("select dim2chain.name, msr12, roundedmsr2 from basecube where " + TWO_DAYS_RANGE, conf);
    String expected1 = getExpectedQuery(cubeName,
        "SELECT (dim2chain.name) as `alias0`, sum((basecube.msr12)) as `alias1`, 0.0 as `alias2` FROM  ",
        " JOIN " + getDbName()
            + "c1_testdim2tbl dim2chain ON basecube.dim2 = "
            + " dim2chain.id and (dim2chain.dt = 'latest') ", null,
        " group by dim2chain.name", null, getWhereForDailyAndHourly2days(cubeName, "C1_testFact2_BASE"));
    String expected2 = getExpectedQuery(cubeName,
        "SELECT (dim2chain.name) as `alias0`, 0.0 as `alias1`, sum((basecube.msr2)) as `alias2` FROM ", " JOIN "
            + getDbName()
            + "c1_testdim2tbl dim2chain ON basecube.dim2 = "
            + " dim2chain.id and (dim2chain.dt = 'latest') ", null,
        " group by dim2chain.name", null, getWhereForHourly2days(cubeName, "C1_testfact1_raw_base"));
    compareContains(expected1, hqlQuery);
    compareContains(expected2, hqlQuery);
    assertTrue(hqlQuery.toLowerCase().startsWith(
      "select (basecube.alias0) as `name`, sum((basecube.alias1)) as `msr12`, "
          + "round((sum((basecube.alias2)) / 1000)) as `roundedmsr2` from"), hqlQuery);
    assertTrue(hqlQuery.contains("UNION ALL") && hqlQuery.endsWith("GROUP BY (basecube.alias0)"),
      hqlQuery);
  }

  @Test
  public void testMultiFactQueryWithDenormColumn() throws Exception {
    // query with denorm variable
    String hqlQuery = rewrite("select dim2, msr13, roundedmsr2 from basecube where " + TWO_DAYS_RANGE, conf);
    String expected1 = getExpectedQuery(cubeName, "select dim2chain.id as `dim2`, max(basecube.msr13) as `msr13` FROM ",
        " JOIN " + getDbName() + "c1_testdim2tbl dim2chain ON basecube.dim12 = "
            + " dim2chain.id and (dim2chain.dt = 'latest') ", null, " group by dim2chain.id", null,
        getWhereForHourly2days(cubeName, "C1_testFact3_RAW_BASE"));
    String expected2 = getExpectedQuery(cubeName,
        "select basecube.dim2 as `dim2`, round(sum(basecube.msr2)/1000) as `roundedmsr2` FROM ", null,
        " group by basecube.dim2", getWhereForHourly2days(cubeName, "C1_testfact1_raw_base"));
    compareContains(expected1, hqlQuery);
    compareContains(expected2, hqlQuery);
    assertTrue(hqlQuery.toLowerCase().startsWith(
      "select coalesce(mq1.dim2, mq2.dim2) dim2, mq2.msr13 msr13, mq1.roundedmsr2 roundedmsr2 from ")
      || hqlQuery.toLowerCase().startsWith(
        "select coalesce(mq1.dim2, mq2.dim2) dim2, mq1.msr13 msr13, mq2.roundedmsr2 roundedmsr2 from "), hqlQuery);
    assertTrue(hqlQuery.contains("mq1 full outer join ") && hqlQuery.endsWith("mq2 on mq1.dim2 <=> mq2.dim2"),
      hqlQuery);
  }

  @Test
  public void testMultiFactQueryWithDenormColumnInWhere() throws Exception {
    // query with denorm variable
    String hqlQuery = rewrite("select dim2, msr13, roundedmsr2 from basecube where dim2 == 10 and " + TWO_DAYS_RANGE,
      conf);
    String expected1 = getExpectedQuery(cubeName, "select dim2chain.id as `dim2`, max(basecube.msr13) as `msr13` FROM ",
      " JOIN " + getDbName() + "c1_testdim2tbl dim2chain ON basecube.dim12 = "
        + " dim2chain.id and (dim2chain.dt = 'latest') ", "dim2chain.id == 10", " group by dim2chain.id", null,
      getWhereForHourly2days(cubeName, "C1_testFact3_RAW_BASE"));
    String expected2 = getExpectedQuery(cubeName,
      "select basecube.dim2 as `dim2`, round(sum(basecube.msr2)/1000) as `roundedmsr2` FROM ", "basecube.dim2 == 10",
      " group by basecube.dim2", getWhereForHourly2days(cubeName, "C1_testfact1_raw_base"));
    compareContains(expected1, hqlQuery);
    compareContains(expected2, hqlQuery);
    assertTrue(hqlQuery.toLowerCase().startsWith(
      "select coalesce(mq1.dim2, mq2.dim2) dim2, mq2.msr13 msr13, mq1.roundedmsr2 roundedmsr2 from ")
      || hqlQuery.toLowerCase().startsWith(
        "select coalesce(mq1.dim2, mq2.dim2) dim2, mq1.msr13 msr13, mq2.roundedmsr2 roundedmsr2 from "), hqlQuery);
    assertTrue(hqlQuery.contains("mq1 full outer join ") && hqlQuery.endsWith("mq2 on mq1.dim2 <=> mq2.dim2"),
      hqlQuery);
  }
  //TODO union : Wrong fact picked
  @Test
  public void testMultiFactQueryWithExpressionInvolvingDenormVariable() throws Exception {
    // query with expression
    // The expression to be answered from denorm columns
    String hqlQuery =
      rewrite(
        "select booleancut, round(sum(msr2)/1000), avg(msr13 + msr14) from basecube where " + TWO_DAYS_RANGE,
        conf);
    String expected1 =
      getExpectedQuery(cubeName, "SELECT (((basecube.dim1) != 'x') and ((dim2chain.id) != 10)) as `alias0`, "
          + "0.0 as `alias1`, avg(((basecube.msr13) + (basecube.msr14))) as `alias2` FROM ", " JOIN " + getDbName()
          + "c1_testdim2tbl dim2chain ON basecube.dim12 = " + " dim2chain.id and (dim2chain.dt = 'latest') ", null,
        " group by basecube.dim1 != 'x' AND dim2chain.id != 10", null,
        getWhereForHourly2days(cubeName, "C1_testfact3_raw_base"));
    String expected2 =
      getExpectedQuery(cubeName, "SELECT (((basecube.dim1) != 'x') and ((basecube.dim2) != 10)) as `alias0`, "
          + "sum((basecube.msr2)) as `alias1`, 0.0 as `alias2` FROM ", null,
        " group by basecube.dim1 != 'x' AND basecube.dim2 != 10",
        getWhereForHourly2days(cubeName, "C1_testfact1_raw_base"));
    compareContains(expected1, hqlQuery);
    compareContains(expected2, hqlQuery);
    assertTrue(hqlQuery.toLowerCase().startsWith("select (basecube.alias0) as `booleancut`, "
        + "round((sum((basecube.alias1)) / 1000)) as `round((sum(msr2) / 1000))`, "
        + "avg((basecube.alias2)) as `avg((msr13 + msr14))` from "), hqlQuery);
    assertTrue(hqlQuery.contains("UNION ALL")
      && hqlQuery.endsWith("GROUP BY (basecube.alias0)"),
      hqlQuery);
  }

  @Test
  public void testMultiFactQueryWithExpressionInvolvingDenormVariableInWhereClause() throws Exception {
    // query with expression
    // The expression to be answered from denorm columns
    String hqlQuery =
      rewrite(
        "select booleancut, round(sum(msr2)/1000), avg(msr13 + msr14) from basecube where booleancut == 'true' and "
          + TWO_DAYS_RANGE, conf);
    String expected1 =
      getExpectedQuery(cubeName, "SELECT (((basecube.dim1) != 'x') and ((dim2chain.id) != 10)) as `alias0`, "
          + "0.0 as `alias1`, avg(((basecube.msr13) + (basecube.msr14))) as `alias2` FROM ", " JOIN " + getDbName()
          + "c1_testdim2tbl dim2chain ON basecube.dim12 = " + " dim2chain.id and (dim2chain.dt = 'latest') ",
        "(basecube.dim1 != 'x' AND dim2chain.id != 10) == true",
        " group by basecube.dim1 != 'x' AND dim2chain.id != 10", null,
        getWhereForHourly2days(cubeName, "C1_testfact3_raw_base"));
    String expected2 =
      getExpectedQuery(cubeName, "SELECT (((basecube.dim1) != 'x') and ((basecube.dim2) != 10)) as `alias0`, "
          + "sum((basecube.msr2)) as `alias1`, 0.0 as `alias2` FROM ",
          "(basecube.dim1 != 'x' AND basecube.dim2 != 10) == true",
          " group by basecube.dim1 != 'x' AND basecube.dim2 != 10",
        getWhereForHourly2days(cubeName, "C1_testfact1_raw_base"));
    compareContains(expected1, hqlQuery);
    compareContains(expected2, hqlQuery);
    assertTrue(hqlQuery.toLowerCase().startsWith("select (basecube.alias0) as `booleancut`, " +
        "round((sum((basecube.alias1)) / 1000)) as `round((sum(msr2) / 1000))`, " +
        "avg((basecube.alias2)) as `avg((msr13 + msr14))` from"), hqlQuery);
    assertTrue(hqlQuery.contains("UNION ALL")
        && hqlQuery.endsWith("GROUP BY (basecube.alias0)"),
      hqlQuery);
  }

  @Test
  public void testMultiFactQueryWithMaterializedExpressions() throws Exception {
    Configuration tconf = new Configuration(conf);
    tconf.set(CubeQueryConfUtil.getValidFactTablesKey("basecube"), "testfact5_base,testfact6_base");
    String hqlQuery =
      rewrite("select booleancut, round(sum(msr2)/1000), msr13 from basecube where " + TWO_DAYS_RANGE, tconf);
    String expected1 =
      getExpectedQuery(cubeName, "SELECT (basecube.booleancut) as `alias0`, 0.0 as `alias1`, "
          + "max((basecube.msr13)) as `alias2` FROM", null, " " +
          "group by basecube.booleancut", getWhereForDailyAndHourly2days(cubeName, "C1_testfact6_base"));
    String expected2 =
      getExpectedQuery(cubeName, "SELECT (basecube.booleancut) as `alias0`, sum((basecube.msr2)) as `alias1`, " +
          "0.0 as `alias2` FROM ", null, " group by basecube.booleancut",
        getWhereForDailyAndHourly2days(cubeName, "C1_testfact5_base"));
    compareContains(expected1, hqlQuery);
    compareContains(expected2, hqlQuery);
    assertTrue(hqlQuery.toLowerCase().startsWith("select (basecube.alias0) as `booleancut`, "
        + "round((sum((basecube.alias1)) / 1000)) as `round((sum(msr2) / 1000))`, "
        + "max((basecube.alias2)) as `msr13` from "), hqlQuery);
    assertTrue(hqlQuery.contains("UNION ALL")
        && hqlQuery.endsWith("GROUP BY (basecube.alias0)"),
      hqlQuery);
  }

  @Test
  public void testMultiFactQueryCaseWhenExpressionWithChainField() throws Exception {
    Configuration tconf = new Configuration(conf);
    String hqlQuery =
      rewrite("select sum(case when dim22 = 'x' then msr12 else 0 end) as case_expr, sum(msr1) from basecube where "
          + TWO_DAYS_RANGE, tconf);
    String expected1 =
      getExpectedQuery(cubeName, "SELECT sum(case  when ((basecube.dim22) = 'x') then (basecube.msr12) else 0 end) "
          + "as `alias0`, 0.0 as `alias1` FROM ", null, null,
          getWhereForHourly2days(cubeName, "C1_testfact2_raw_base"));
    String expected2 =
      getExpectedQuery(cubeName, "SELECT 0.0 as `alias0`, sum((basecube.msr1)) as `alias1` FROM ", null, null,
        getWhereForHourly2days(cubeName, "c1_testfact1_raw_base"));
    compareContains(expected1, hqlQuery);
    compareContains(expected2, hqlQuery);
    assertTrue(hqlQuery.toLowerCase().startsWith("select sum((basecube.alias0)) as `case_expr`, sum((basecube.alias1)) "
        + "as `sum(msr1)` from "), hqlQuery);
    assertTrue(hqlQuery.contains("UNION ALL") && hqlQuery.endsWith("basecube"), hqlQuery);
  }

  @Test
  public void testMultiFactQueryCaseWhenExpression() throws Exception {
    Configuration tconf = new Configuration(conf);
    String hqlQuery =
      rewrite("select sum(case when dim13 = 'x' then msr12 else 0 end) as case_expr, sum(msr1) from basecube where "
        + TWO_DAYS_RANGE, tconf);
    String expected1 =
      getExpectedQuery(cubeName, "SELECT sum(case  when ((basecube.dim13) = 'x') then (basecube.msr12) else 0 end) "
          + "as `alias0`, 0.0 as `alias1` FROM ", null, null, getWhereForHourly2days(cubeName, "C1_testfact2_raw_base"));
    String expected2 =
      getExpectedQuery(cubeName, "SELECT 0.0 as `alias0`, sum((basecube.msr1)) as `alias1` FROM ", null, null,
        getWhereForHourly2days(cubeName, "c1_testfact1_raw_base"));
    compareContains(expected1, hqlQuery);
    compareContains(expected2, hqlQuery);
    assertTrue(hqlQuery.toLowerCase().startsWith("select sum((basecube.alias0)) as `case_expr`, "
        + "sum((basecube.alias1)) as `sum(msr1)` from "), hqlQuery);
    assertTrue(hqlQuery.contains("UNION ALL") && hqlQuery.endsWith("basecube"), hqlQuery);
  }

  @Test
  public void testMultiFactQueryCaseWhenExpressionWithGroupby() throws Exception {
    Configuration tconf = new Configuration(conf);
    String hqlQuery =
      rewrite("select dim1, sum(case when dim13 = 'x' then msr12 else 0 end) as case_expr, sum(msr1) from basecube "
        + "where " + TWO_DAYS_RANGE, tconf);
    String expected1 =
      getExpectedQuery(cubeName, "SELECT (basecube.dim1) as `alias0`, sum(case  when ((basecube.dim13) = 'x') "
          + "then (basecube.msr12) else 0 end) as `alias1`, 0.0 as `alias2` FROM ", null, " group by basecube.dim1 ",
        getWhereForHourly2days(cubeName, "C1_testfact2_raw_base"));
    String expected2 =
      getExpectedQuery(cubeName, "SELECT (basecube.dim1) as `alias0`, 0.0 as `alias1`, sum((basecube.msr1)) "
          + "as `alias2` FROM", null,
        " group by basecube.dim1 ", getWhereForHourly2days(cubeName, "c1_testfact1_raw_base"));
    compareContains(expected1, hqlQuery);
    compareContains(expected2, hqlQuery);
    assertTrue(hqlQuery.toLowerCase().startsWith(
      "select (basecube.alias0) as `dim1`, sum((basecube.alias1)) as `case_expr`, sum((basecube.alias2)) as `sum(msr1)` from")
        , hqlQuery);
    assertTrue(hqlQuery.contains("UNION ALL") && hqlQuery.endsWith("GROUP BY (basecube.alias0)"),
      hqlQuery);
  }

  @Test
  public void testMultiFactQueryCaseWhenExpressionWithHavingClause() throws Exception {
    Configuration tconf = new Configuration(conf);
    String hqlQuery =
      rewrite("select sum(case when dim13 = 'x' then msr12 else 0 end) as case_expr, sum(msr1) from basecube "
        + "where "
        + TWO_DAYS_RANGE + " having sum(case when dim13 = 'x' then msr12 else 0 end) > 100 and sum(msr1) > 500", tconf);
    String expected1 =
      getExpectedQuery(cubeName, "SELECT sum(case  when ((basecube.dim13) = 'x') then (basecube.msr12) else 0 end) "
          + "as `alias0`, 0.0 as `alias1` FROM ", null, "", getWhereForHourly2days(cubeName, "C1_testfact2_raw_base"));
    String expected2 =
      getExpectedQuery(cubeName, "SELECT 0.0 as `alias0`, sum((basecube.msr1)) as `alias1` FROM ", null, "",
        getWhereForHourly2days(cubeName, "c1_testfact1_raw_base"));
    compareContains(expected1, hqlQuery);
    compareContains(expected2, hqlQuery);
    assertTrue(hqlQuery.toLowerCase().startsWith("select sum((basecube.alias0)) as `case_expr`, sum((basecube.alias1)) "
        + "as `sum(msr1)` from"), hqlQuery);
    assertTrue(hqlQuery.contains("UNION ALL") && hqlQuery.endsWith("HAVING ((sum((basecube.alias0)) > 100) "
        + "and (sum((basecube.alias1)) > 500))"), hqlQuery);
  }

  @Test
  public void testMultiFactQueryCaseWhenExpressionWithGroubyAndHavingClause() throws Exception {
    Configuration tconf = new Configuration(conf);
    String hqlQuery =
      rewrite("select dim1, sum(case when dim13 = 'x' then msr12 else 0 end) as case_expr, sum(msr1) from basecube "
        + "where "
        + TWO_DAYS_RANGE + " having sum(case when dim13 = 'x' then msr12 else 0 end) > 100 and sum(msr1) > 500", tconf);
    String expected1 =
      getExpectedQuery(cubeName, "SELECT (basecube.dim1) as `alias0`, sum(case  when ((basecube.dim13) = 'x') then " +
          "(basecube.msr12) else 0 end) as `alias1`, 0.0 as `alias2` FROM", null, " group by basecube.dim1",
          getWhereForHourly2days(cubeName, "C1_testfact2_raw_base"));
    String expected2 =
      getExpectedQuery(cubeName, "SELECT (basecube.dim1) as `alias0`, 0.0 as `alias1`, sum((basecube.msr1)) "
          + "as `alias2` FROM", null, " group by basecube.dim1",
          getWhereForHourly2days(cubeName, "c1_testfact1_raw_base"));
    compareContains(expected1, hqlQuery);
    compareContains(expected2, hqlQuery);
    assertTrue(hqlQuery.toLowerCase().startsWith(
      "select (basecube.alias0) as `dim1`, sum((basecube.alias1)) as `case_expr`, sum((basecube.alias2)) as `sum(msr1)` from")
        , hqlQuery);
    assertTrue(hqlQuery.contains("UNION ALL")
        && hqlQuery.endsWith("HAVING ((sum((basecube.alias1)) > 100) and (sum((basecube.alias2)) > 500))"), hqlQuery);
  }

  // TODO union : Fix after MaxCoveringSet resolver
  /*
  @Test
  public void testFallbackPartCol() throws Exception {
    Configuration conf = getConfWithStorages("C1");
    conf.setBoolean(CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, false);
    String hql, expected;
    // Prefer fact that has a storage with part col on queried time dim
    hql = rewrite("select msr12 from basecube where " + TWO_DAYS_RANGE, conf);
    expected = getExpectedQuery(BASE_CUBE_NAME, "select sum(basecube.msr12) FROM ", null, null,
      getWhereForDailyAndHourly2days(BASE_CUBE_NAME, "c1_testfact2_base"));
    compareQueries(hql, expected);

    // If going to fallback timedim, and partitions are missing, then error should be missing partition on that
    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C4");
    conf.setBoolean(CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, true);
    LensException exc =
      getLensExceptionInRewrite("select msr12 from basecube where " + TWO_DAYS_RANGE, conf);
    NoCandidateFactAvailableException ne = (NoCandidateFactAvailableException) exc;
    PruneCauses.BriefAndDetailedError pruneCause = ne.getJsonMessage();
    assertTrue(pruneCause.getBrief().contains("Missing partitions"));
    assertEquals(pruneCause.getDetails().get("testfact2_base").iterator().next().getCause(), MISSING_PARTITIONS);
    assertEquals(pruneCause.getDetails().get("testfact2_base").iterator().next().getMissingPartitions().size(), 1);
    assertEquals(
      pruneCause.getDetails().get("testfact2_base").iterator().next().getMissingPartitions().iterator().next(),
      "ttd:["
        + UpdatePeriod.SECONDLY.format(DateUtils.addDays(DateUtils.truncate(TWODAYS_BACK, Calendar.HOUR), -10))
        + ", " + UpdatePeriod.SECONDLY.format(DateUtils.addDays(DateUtils.truncate(NOW, Calendar.HOUR), 10))
        + ")");

    // fail on partial false. Should go to fallback column. Also testing transitivity of timedim relations
    conf.setBoolean(CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, false);
    hql = rewrite("select msr12 from basecube where " + TWO_DAYS_RANGE, conf);
    String dTimeWhereClause = "basecube.d_time >= '" + HIVE_QUERY_DATE_PARSER.get().format(ABSDATE_PARSER.get().parse(
      getAbsDateFormatString(getDateUptoHours(
        TWODAYS_BACK)))) + "' and "
      + "basecube.d_time < '" + HIVE_QUERY_DATE_PARSER.get().format(ABSDATE_PARSER.get().parse(
        getAbsDateFormatString(getDateUptoHours(NOW))));
    String pTimeWhereClause = "basecube.processing_time >= '"
      + HIVE_QUERY_DATE_PARSER.get().format(ABSDATE_PARSER.get().parse(
        getAbsDateFormatString(getDateUptoHours(
          DateUtils.addDays(TWODAYS_BACK, -5))))) + "' and "
        + "basecube.processing_time < '" + HIVE_QUERY_DATE_PARSER.get().format(ABSDATE_PARSER.get().parse(
          getAbsDateFormatString(getDateUptoHours(DateUtils.addDays(NOW, 5)))));
    expected = getExpectedQuery(BASE_CUBE_NAME, "select sum(basecube.msr12) FROM ", null,
        " and " + dTimeWhereClause + " and " + pTimeWhereClause,
      getWhereForDailyAndHourly2daysWithTimeDim(BASE_CUBE_NAME, "ttd",
        DateUtils.addDays(TWODAYS_BACK, -10), DateUtils.addDays(NOW, 10), "c4_testfact2_base"));
    compareQueries(hql, expected);

    // Multiple timedims in single query. test that
    CubeQueryContext ctx =
      rewriteCtx("select msr12 from basecube where " + TWO_DAYS_RANGE + " and " + TWO_DAYS_RANGE_TTD, conf);
    assertEquals(ctx.getCandidateFactSets().size(), 1);
    assertEquals(ctx.getCandidateFactSets().iterator().next().size(), 1);
    CandidateFact cfact = ctx.getCandidateFactSets().iterator().next().iterator().next();

    assertEquals(cfact.getRangeToStoragePartMap().size(), 2);
    Set<String> storages = Sets.newHashSet();
    for(Map<String, String> entry: cfact.getRangeToStorageWhereMap().values()) {
      storages.addAll(entry.keySet());
    }
    assertEquals(storages.size(), 1);
    String storage = storages.iterator().next();
    for(Map.Entry<TimeRange, Map<String, String>> entry: cfact.getRangeToStorageWhereMap().entrySet()) {
      if (entry.getKey().getPartitionColumn().equals("dt")) {
        ASTNode parsed = HQLParser.parseExpr(entry.getValue().get(storage));
        assertEquals(parsed.getToken().getType(), KW_AND);
        assertTrue(entry.getValue().get(storage).substring(((CommonToken) parsed.getToken()).getStopIndex() + 1)
          .toLowerCase().contains(dTimeWhereClause));
        assertFalse(entry.getValue().get(storage).substring(0, ((CommonToken) parsed.getToken()).getStartIndex())
          .toLowerCase().contains("and"));
      } else if (entry.getKey().getPartitionColumn().equals("ttd")) {
        assertFalse(entry.getValue().get(storage).toLowerCase().contains("and"));
      } else {
        throw new LensException("Unexpected");
      }
    }
  }
  */
  @Test
  public void testMultiFactQueryWithHaving() throws Exception {

    String hqlQuery, expected1, expected2;
    String endSubString = "mq2 on mq1.dim1 <=> mq2.dim1 AND mq1.dim11 <=> mq2.dim11";
    String joinSubString = "mq1 full outer join ";

    // only One having clause, that too answerable from one fact
    hqlQuery = rewrite("select dim1, dim11, msr12 from basecube where " + TWO_DAYS_RANGE
      + "having roundedmsr2 > 0", conf);
    expected1 = getExpectedQuery(cubeName,
      "SELECT (basecube.dim1) as `alias0`, (basecube.dim11) as `alias1`, sum((basecube.msr12)) " +
          "as `alias2`, 0.0 as `alias3` FROM ", null, " group by basecube.dim1, basecube.dim11",
      getWhereForDailyAndHourly2days(cubeName, "C1_testFact2_BASE"));
    expected2 = getExpectedQuery(cubeName,
      "SELECT (basecube.dim1) as `alias0`, (basecube.dim11) as `alias1`, 0.0 as `alias2`, "
          + "sum((basecube.msr2)) as `alias3` FROM ", null, " group by basecube.dim1, basecube.dim11",
      getWhereForDailyAndHourly2days(cubeName, "C1_testFact1_BASE"));

    assertTrue(hqlQuery.toLowerCase().contains("group by (basecube.alias0), (basecube.alias1)"));
    compareContains(expected1, hqlQuery);
    compareContains(expected2, hqlQuery);
    assertTrue(hqlQuery.toLowerCase().startsWith("select (basecube.alias0) as `dim1`, (basecube.alias1) as `dim11`, "
        + "sum((basecube.alias2)) as `msr12` from"), hqlQuery);
    assertTrue(hqlQuery.endsWith("HAVING (round((sum((basecube.alias3)) / 1000)) > 0)"));

    // Two having clause, one from each fact.
    hqlQuery = rewrite("select dim1, dim11, msr12, roundedmsr2 from basecube where " + TWO_DAYS_RANGE
      + "having msr12 > 2 and roundedmsr2 > 0", conf);
    expected1 = getExpectedQuery(cubeName,
      "SELECT (basecube.dim1) as `alias0`, (basecube.dim11) as `alias1`, sum((basecube.msr12)) as `alias2`, "
          + "0.0 as `alias3` FROM ", null, " group by basecube.dim1, basecube.dim11",
      getWhereForDailyAndHourly2days(cubeName, "C1_testFact2_BASE"));
    expected2 = getExpectedQuery(cubeName,
      "SELECT (basecube.dim1) as `alias0`, (basecube.dim11) as `alias1`, 0.0 as `alias2`, " +
          "sum((basecube.msr2)) as `alias3` FROM ", null, " group by basecube.dim1, basecube.dim11",
      getWhereForDailyAndHourly2days(cubeName, "C1_testFact1_BASE"));

    compareContains(expected1, hqlQuery);
    compareContains(expected2, hqlQuery);
    assertTrue(hqlQuery.toLowerCase().startsWith("select (basecube.alias0) as `dim1`, (basecube.alias1) as `dim11`, "
        + "sum((basecube.alias2)) as `msr12`, round((sum((basecube.alias3)) / 1000)) as `roundedmsr2` from"),
        hqlQuery);
    assertTrue(hqlQuery.endsWith("HAVING ((sum((basecube.alias2)) > 2) "
        + "and (round((sum((basecube.alias3)) / 1000)) > 0))"));

    // Two having clauses and one complex expression in having which needs to be split over the two facts
    // And added as where clause outside
    //TODO union : floor is not a valid function.
    /*
    (((tok_function(sum((basecube.msr12))) + round((sum((basecube.alias3)) / 1000))) <= 1000)
     and (sum((basecube.alias2)) > 2) and (round((sum((basecube.alias3)) / 1000)) > 0))
           <= [LESSTHANOREQUALTO] (l3c1p145) {
        + [PLUS] (l4c1p132) {
          TOK_FUNCTION [TOK_FUNCTION] (l5c1p0) {
            TOK_FUNCTION [TOK_FUNCTION] (l6c1p0) {
              SUM [Identifier] (l7c1p0)$
              . [DOT] (l7c2p0) {
                TOK_TABLE_OR_COL [TOK_TABLE_OR_COL] (l8c1p0) {
                  basecube [Identifier] (l9c1p0)$
                }
                alias2 [Identifier] (l8c2p0)$
              }
            }
            TOK_FUNCTION [TOK_FUNCTION] (l6c2p0) {
              SUM [Identifier] (l7c1p0)$
              . [DOT] (l7c2p0) {
                TOK_TABLE_OR_COL [TOK_TABLE_OR_COL] (l8c1p0) {
                  basecube [Identifier] (l9c1p0)$
                }
                msr12 [Identifier] (l8c2p0)$
              }
            }
          }
          TOK_FUNCTION [TOK_FUNCTION] (l5c2p0) {
            round [Identifier] (l6c1p0)$
            / [DIVIDE] (l6c2p10) {
              TOK_FUNCTION [TOK_FUNCTION] (l7c1p0) {
                SUM [Identifier] (l8c1p0)$
                . [DOT] (l8c2p0) {
                  TOK_TABLE_OR_COL [TOK_TABLE_OR_COL] (l9c1p0) {
                    basecube [Identifier] (l10c1p0)$
                  }
                  alias3 [Identifier] (l9c2p0)$
                }
              }
              1000 [Number] (l7c2p11)$
            }
          }
        }
        1000 [Number] (l4c2p148)$
      }
     */
    hqlQuery = rewrite("select dim1, dim11, msr12, roundedmsr2 from basecube where " + TWO_DAYS_RANGE
      + "having msr12+roundedmsr2 <= 1000 and msr12 > 2 and roundedmsr2 > 0", conf);
        expected1 = getExpectedQuery(cubeName,
      "SELECT (basecube.dim1) as `alias0`, (basecube.dim11) as `alias1`, sum((basecube.msr12)) as `alias2`, "
          + "0.0 as `alias3` FROM ", null, " group by basecube.dim1, basecube.dim11",
      getWhereForDailyAndHourly2days(cubeName, "C1_testFact2_BASE"));

    expected2 = getExpectedQuery(cubeName,
        "SELECT (basecube.dim1) as `alias0`, (basecube.dim11) as `alias1`, 0.0 as `alias2`, "
            + "sum((basecube.msr2)) as `alias3` FROM ", null, " group by basecube.dim1, basecube.dim11",
        getWhereForDailyAndHourly2days(cubeName, "C1_testFact1_BASE"));

    compareContains(expected1, hqlQuery);
    compareContains(expected2, hqlQuery);
    assertTrue(hqlQuery.toLowerCase().startsWith("select (basecube.alias0) as `dim1`, (basecube.alias1) as `dim11`, "
        + "sum((basecube.alias2)) as `msr12`, round((sum((basecube.alias3)) / 1000)) as `roundedmsr2` from"), hqlQuery);
    assertTrue(hqlQuery.endsWith("(((sum((basecube.alias2)) + round((sum((basecube.alias3)) / 1000))) <= 1000) "
        + "and (sum((basecube.alias2)) > 2) and (round((sum((basecube.alias3)) / 1000)) > 0))"), hqlQuery);

    // TODO union : why?, columns are projected can't be part of having!
    // No push-down-able having clauses.
    hqlQuery = rewrite("select dim1, dim11, msr12, roundedmsr2 from basecube where " + TWO_DAYS_RANGE
      + "having msr12+roundedmsr2 <= 1000", conf);
    expected1 = getExpectedQuery(cubeName,
      "SELECT (basecube.dim1) as `alias0`, (basecube.dim11) as `alias1`, sum((basecube.msr12)) as `alias2`, " +
          "0.0 as `alias3` FROM", null, " group by basecube.dim1, basecube.dim11",
      getWhereForDailyAndHourly2days(cubeName, "C1_testFact2_BASE"));
    expected2 = getExpectedQuery(cubeName,
      "SELECT (basecube.dim1) as `alias0`, (basecube.dim11) as `alias1`, 0.0 as `alias2`, sum((basecube.msr2)) " +
          "as `alias3` FROM", null, " group by basecube.dim1, basecube.dim11",
      getWhereForDailyAndHourly2days(cubeName, "C1_testFact1_BASE"));

    compareContains(expected1, hqlQuery);
    compareContains(expected2, hqlQuery);
    assertTrue(hqlQuery.toLowerCase().startsWith("select (basecube.alias0) as `dim1`, (basecube.alias1) as `dim11`, "
        + "sum((basecube.alias2)) as `msr12`, round((sum((basecube.alias3)) / 1000)) as `roundedmsr2` from"), hqlQuery);
    assertTrue(hqlQuery.contains("UNION ALL")
      && hqlQuery.endsWith("HAVING ((sum((basecube.alias2)) + " +
        "round((sum((basecube.alias3)) / 1000))) <= 1000)"), hqlQuery);

    // function over expression of two functions over measures
    hqlQuery = rewrite("select dim1, dim11, msr12, roundedmsr2 from basecube where " + TWO_DAYS_RANGE
      + "having round(msr12+roundedmsr2) <= 1000", conf);
    expected1 = getExpectedQuery(cubeName,
      "SELECT (basecube.dim1) as `alias0`, (basecube.dim11) as `alias1`, sum((basecube.msr12)) as `alias2`, " +
          "0.0 as `alias3` FROM ", null, " group by basecube.dim1, basecube.dim11",
      getWhereForDailyAndHourly2days(cubeName, "C1_testFact2_BASE"));
    expected2 = getExpectedQuery(cubeName,
      " SELECT (basecube.dim1) as `alias0`, (basecube.dim11) as `alias1`, 0.0 as `alias2`, " +
          "sum((basecube.msr2)) as `alias3` FROM ", null, " group by basecube.dim1, basecube.dim11",
      getWhereForDailyAndHourly2days(cubeName, "C1_testFact1_BASE"));

    compareContains(expected1, hqlQuery);
    compareContains(expected2, hqlQuery);
    assertTrue(hqlQuery.toLowerCase().startsWith("select (basecube.alias0) as `dim1`, (basecube.alias1) as `dim11`, "
        + "sum((basecube.alias2)) as `msr12`, round((sum((basecube.alias3)) / 1000)) as `roundedmsr2` from"), hqlQuery);
    assertTrue(hqlQuery.contains("UNION ALL")
      && hqlQuery.endsWith(" HAVING (round((sum((basecube.alias2)) + " +
        "round((sum((basecube.alias3)) / 1000)))) <= 1000)"), hqlQuery);


    // Following test cases only select dimensions, and all the measures are in having.
    // Mostly tests follow the same pattern as the above tests,
    // The extra thing to test is the inclusion of sub-expressions in select clauses.

    hqlQuery = rewrite("select dim1, dim11 from basecube where " + TWO_DAYS_RANGE
      + "having msr12 > 2 and roundedmsr2 > 0", conf);
    expected1 = getExpectedQuery(cubeName,
      "SELECT (basecube.dim1) as `alias0`, (basecube.dim11) as `alias1`, sum((basecube.msr12)) as `alias2`, "
          + "0.0 as `alias3` FROM ", null, " group by basecube.dim1, basecube.dim11",
      getWhereForDailyAndHourly2days(cubeName, "C1_testFact2_BASE"));
    expected2 = getExpectedQuery(cubeName,
      "SELECT (basecube.dim1) as `alias0`, (basecube.dim11) as `alias1`, 0.0 as `alias2`, " +
          "sum((basecube.msr2)) as `alias3` FROM ", null, " group by basecube.dim1, basecube.dim11",
      getWhereForDailyAndHourly2days(cubeName, "C1_testFact1_BASE"));
    String begin = "select (basecube.alias0) as `dim1`, (basecube.alias1) as `dim11` from";
    compareContains(expected1, hqlQuery);
    compareContains(expected2, hqlQuery);
    assertTrue(hqlQuery.toLowerCase().startsWith(begin), hqlQuery);
    assertTrue(hqlQuery.contains("UNION ALL")
        && hqlQuery.endsWith("HAVING ((sum((basecube.alias2)) > 2) "
        + "and (round((sum((basecube.alias3)) / 1000)) > 0))"), hqlQuery);

    hqlQuery = rewrite("select dim1, dim11 from basecube where " + TWO_DAYS_RANGE
      + "having msr12 > 2 and roundedmsr2 > 0 and msr2 > 100", conf);
    expected1 = getExpectedQuery(cubeName,
        "SELECT (basecube.dim1) as `alias0`, (basecube.dim11) as `alias1`, sum((basecube.msr12)) as `alias2`, " +
            "0.0 as `alias3`, 0.0 as `alias4` FROM ", null, " group by basecube.dim1, basecube.dim11",
        getWhereForDailyAndHourly2days(cubeName, "C1_testFact2_BASE"));
    expected2 = getExpectedQuery(cubeName,
      "SELECT (basecube.dim1) as `alias0`, (basecube.dim11) as `alias1`, 0.0 as `alias2`, " +
          "sum((basecube.msr2)) as `alias3`, sum((basecube.msr2)) as `alias4` FROM ", null,
      " group by basecube.dim1, basecube.dim11",
      getWhereForDailyAndHourly2days(cubeName, "C1_testFact1_BASE"));

    compareContains(expected1, hqlQuery);
    compareContains(expected2, hqlQuery);
    assertTrue(hqlQuery.toLowerCase().startsWith("select (basecube.alias0) as `dim1`, " +
        "(basecube.alias1) as `dim11` from"), hqlQuery);
    assertTrue(hqlQuery.contains("UNION ALL") && hqlQuery.endsWith("HAVING ((sum((basecube.alias2)) > 2) and " +
        "(round((sum((basecube.alias4)) / 1000)) > 0) and (sum((basecube.alias4)) > 100))"), hqlQuery);

    hqlQuery = rewrite("select dim1, dim11 from basecube where " + TWO_DAYS_RANGE
      + "having msr12+roundedmsr2 <= 1000", conf);
    expected1 = getExpectedQuery(cubeName,
      "SELECT (basecube.dim1) as `alias0`, (basecube.dim11) as `alias1`, sum((basecube.msr12)) "
          + "as `alias2`, 0.0 as `alias3` FROM ",
      null, " group by basecube.dim1, basecube.dim11",
      getWhereForDailyAndHourly2days(cubeName, "C1_testFact2_BASE"));
    expected2 = getExpectedQuery(cubeName,
      "SELECT (basecube.dim1) as `alias0`, (basecube.dim11) as `alias1`, 0.0 as `alias2`, " +
          "sum((basecube.msr2)) as `alias3` FROM ",
      null, " group by basecube.dim1, basecube.dim11",
      getWhereForDailyAndHourly2days(cubeName, "C1_testFact1_BASE"));

    compareContains(expected1, hqlQuery);
    compareContains(expected2, hqlQuery);
    assertTrue(hqlQuery.toLowerCase().startsWith("select (basecube.alias0) as `dim1`, (basecube.alias1) " +
        "as `dim11` from"), hqlQuery);
    assertTrue(hqlQuery.contains("UNION ALL")
      && hqlQuery.endsWith("HAVING ((sum((basecube.alias2)) + round((sum((basecube.alias3)) / 1000))) <= 1000)"),
        hqlQuery);

    hqlQuery = rewrite("select dim1, dim11 from basecube where " + TWO_DAYS_RANGE
      + "having msr12 > 2 and roundedmsr2 > 0 and msr12+roundedmsr2 <= 1000", conf);
    expected1 = getExpectedQuery(cubeName,
      "SELECT (basecube.dim1) as `alias0`, (basecube.dim11) as `alias1`, sum((basecube.msr12))"
          + " as `alias2`, 0.0 as `alias3`, sum((basecube.msr12)) as `alias4`, 0.0 as `alias5` FROM ",
      null, " group by basecube.dim1, basecube.dim11",
      getWhereForDailyAndHourly2days(cubeName, "C1_testFact2_BASE"));
    expected2 = getExpectedQuery(cubeName,
      "SELECT (basecube.dim1) as `alias0`, (basecube.dim11) as `alias1`, 0.0 as `alias2`, sum((basecube.msr2)) "
          + "as `alias3`, 0.0 as `alias4`, sum((basecube.msr2)) as `alias5` FROM ",
      null, " group by basecube.dim1, basecube.dim11",
      getWhereForDailyAndHourly2days(cubeName, "C1_testFact1_BASE"));

    compareContains(expected1, hqlQuery);
    compareContains(expected2, hqlQuery);
    assertTrue(hqlQuery.toLowerCase().startsWith("select (basecube.alias0) as `dim1`, (basecube.alias1) "
        + "as `dim11` from "), hqlQuery);
    assertTrue(hqlQuery.contains("UNION ALL")
      && hqlQuery.endsWith("HAVING ((sum((basecube.alias4)) > 2) and (round((sum((basecube.alias5)) / 1000)) > 0) "
        + "and ((sum((basecube.alias4)) + round((sum((basecube.alias5)) / 1000))) <= 1000))"), hqlQuery);


    hqlQuery = rewrite("select dim1, dim11 from basecube where " + TWO_DAYS_RANGE
      + "having msr12 > 2 or roundedmsr2 > 0 or msr12+roundedmsr2 <= 1000", conf);
    expected1 = getExpectedQuery(cubeName,
      "SELECT (basecube.dim1) as `alias0`, (basecube.dim11) as `alias1`, sum((basecube.msr12)) as `alias2`, " +
          "0.0 as `alias3`, sum((basecube.msr12)) as `alias4`, 0.0 as `alias5` FROM ",
      null, " group by basecube.dim1, basecube.dim11",
      getWhereForDailyAndHourly2days(cubeName, "C1_testFact2_BASE"));
    expected2 = getExpectedQuery(cubeName,
      "SELECT (basecube.dim1) as `alias0`, (basecube.dim11) as `alias1`, 0.0 as `alias2`, sum((basecube.msr2)) "
          + "as `alias3`, 0.0 as `alias4`, sum((basecube.msr2)) as `alias5` FROM ",
      null, " group by basecube.dim1, basecube.dim11",
      getWhereForDailyAndHourly2days(cubeName, "C1_testFact1_BASE"));

    compareContains(expected1, hqlQuery);
    compareContains(expected2, hqlQuery);
    assertTrue(hqlQuery.toLowerCase().startsWith("select (basecube.alias0) as `dim1`, (basecube.alias1) "
        + "as `dim11` from"), hqlQuery);
    assertTrue(hqlQuery.contains("UNION ALL")
      && hqlQuery.endsWith("HAVING ((sum((basecube.alias4)) > 2) or (round((sum((basecube.alias5)) / 1000)) > 0) or "
        + "((sum((basecube.alias4)) + round((sum((basecube.alias5)) / 1000))) <= 1000))"), hqlQuery);
  }
}
