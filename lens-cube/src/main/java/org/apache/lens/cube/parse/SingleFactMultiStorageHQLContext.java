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

import static org.apache.lens.cube.parse.HQLParser.getString;

import static org.apache.hadoop.hive.ql.parse.HiveParser.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.lens.cube.metadata.Dimension;
import org.apache.lens.server.api.error.LensException;

import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;

import org.antlr.runtime.CommonToken;
import org.antlr.runtime.tree.Tree;

import lombok.Data;


public class SingleFactMultiStorageHQLContext extends UnionHQLContext {

  int aliasCounter = 0;

  @Data
  public static class HashableASTNode {
    private ASTNode ast;
    private int hashCode = -1;
    private boolean hashCodeComputed = false;

    public HashableASTNode(ASTNode ast) {
      this.ast = ast;
    }

    public void setAST(ASTNode ast) {
      this.ast = ast;
      hashCodeComputed = false;
    }

    public ASTNode getAST() {
      return ast;
    }

    @Override
    public int hashCode() {
      if (!hashCodeComputed) {
        hashCode = HQLParser.getString(ast).hashCode();
        hashCodeComputed = true;
      }
      return hashCode;
    }

    @Override
    public boolean equals(Object o) {
      return o instanceof HashableASTNode && this.hashCode() == o.hashCode() && HQLParser.getString(this.getAST())
        .trim().equalsIgnoreCase(HQLParser.getString(((HashableASTNode) o).getAST()).trim());
    }
  }

  private Map<HashableASTNode, ASTNode> innerToOuterASTs = new HashMap<>();

  SingleFactMultiStorageHQLContext(CandidateFact fact, Map<Dimension, CandidateDim> dimsToQuery, CubeQueryContext query)
    throws LensException {
    super(query, fact);
    processSelectAST();
    processGroupByAST();
    processHavingAST();
    processOrderByAST();
    processLimit();
    setHqlContexts(getUnionContexts(fact, dimsToQuery, query));
  }

  private void processSelectAST() {
    query.getSelectFinalAliases().clear();
    ASTNode originalSelectAST = HQLParser.copyAST(query.getSelectAST());
    query.setSelectAST(new ASTNode(originalSelectAST.getToken()));
    ASTNode outerSelectAST = processExpression(originalSelectAST);
    setSelect(HQLParser.getString(outerSelectAST));
  }

  private void processGroupByAST() {
    if (query.getGroupByAST() != null) {
      setGroupby(getString(processExpression(query.getGroupByAST())));
    }
  }

  private void processHavingAST() throws LensException {
    if (query.getHavingAST() != null) {
      setHaving(HQLParser.getString(processExpression(query.getHavingAST())));
      query.setHavingAST(null);
    }
  }

  private void processOrderByAST() {
    if (query.getOrderByAST() != null) {
      setOrderby(HQLParser.getString(processExpression(query.getOrderByAST())));
      query.setOrderByAST(null);
    }
  }

  private void processLimit() {
    setLimit(query.getLimitValue());
    query.setLimitValue(null);
  }
  /*
  Perform a DFS on the provided AST, and Create an AST of similar structure with changes specific to the
  inner query - outer query dynamics. The resultant AST is supposed to be used in outer query.

  Base cases:
   1. ast is null => null
   2. ast is table.column => add this to inner select expressions, generate alias, return cube.alias. Memoize the
            mapping table.column => cube.alias
   3. ast is aggregate_function(table.column) => add aggregate_function(table.column) to inner select expressions,
            generate alias, return aggregate_function(cube.alias). Memoize the mapping
            aggregate_function(table.column) => aggregate_function(cube.alias)
            Assumption is aggregate_function is transitive i.e. f(a,b,c,d) = f(f(a,b), f(c,d)). SUM, MAX, MIN etc
            are transitive, while AVG, COUNT etc are not. For non-transitive aggregate functions, the re-written
            query will be incorrect.
   4. If given ast is memoized as mentioned in the above cases, return the mapping.

   Recursive case:
     Copy the root node, process children recursively and add as children to the copied node. Return the copied node.
   */
  private ASTNode processExpression(ASTNode astNode) {
    if (astNode == null) {
      return null;
    }
    if (innerToOuterASTs.containsKey(new HashableASTNode(astNode))) {
      return innerToOuterASTs.get(new HashableASTNode(astNode));
    }
    if (HQLParser.isAggregateAST(astNode)) {
      ASTNode innerSelectASTWithoutAlias = HQLParser.copyAST(astNode);
      ASTNode innerSelectExprAST = new ASTNode(new CommonToken(HiveParser.TOK_SELEXPR));
      innerSelectExprAST.addChild(innerSelectASTWithoutAlias);
      String alias = decideAlias(astNode);
      ASTNode aliasNode = new ASTNode(new CommonToken(Identifier, alias));
      innerSelectExprAST.addChild(aliasNode);
      addToInnerSelectAST(innerSelectExprAST);
      ASTNode dotAST = getDotAST(query.getCube().getName(), alias);
      ASTNode outerAST = new ASTNode(new CommonToken(TOK_FUNCTION));
      outerAST.addChild(new ASTNode(new CommonToken(Identifier, astNode.getChild(0).getText())));
      outerAST.addChild(dotAST);
      innerToOuterASTs.put(new HashableASTNode(innerSelectASTWithoutAlias), outerAST);
      return outerAST;
    } else if (HQLParser.isTableColumnAST(astNode)) {
      ASTNode innerSelectASTWithoutAlias = HQLParser.copyAST(astNode);
      ASTNode innerSelectExprAST = new ASTNode(new CommonToken(HiveParser.TOK_SELEXPR));
      innerSelectExprAST.addChild(innerSelectASTWithoutAlias);
      String alias = decideAlias(astNode);
      ASTNode aliasNode = new ASTNode(new CommonToken(Identifier, alias));
      innerSelectExprAST.addChild(aliasNode);
      addToInnerSelectAST(innerSelectExprAST);
      ASTNode outerAST = getDotAST(query.getCube().getName(), alias);
      innerToOuterASTs.put(new HashableASTNode(innerSelectASTWithoutAlias), outerAST);
      return outerAST;
    } else {
      ASTNode outerHavingExpression = new ASTNode(astNode);
      if (astNode.getChildren() != null) {
        for (Node child : astNode.getChildren()) {
          outerHavingExpression.addChild(processExpression((ASTNode) child));
        }
      }
      return outerHavingExpression;
    }
  }

  private void addToInnerSelectAST(ASTNode selectExprAST) {
    if (query.getSelectAST() == null) {
      query.setSelectAST(new ASTNode(new CommonToken(TOK_SELECT)));
    }
    query.getSelectAST().addChild(selectExprAST);
  }

  private ASTNode getDotAST(String tableAlias, String fieldAlias) {
    ASTNode child = new ASTNode(new CommonToken(DOT, "."));
    child.addChild(new ASTNode(new CommonToken(TOK_TABLE_OR_COL, "TOK_TABLE_OR_COL")));
    child.getChild(0).addChild(new ASTNode(new CommonToken(Identifier, tableAlias)));
    child.addChild(new ASTNode(new CommonToken(Identifier, fieldAlias)));
    return child;
  }

  private String decideAlias(Tree child) {
    // Can add intelligence in aliases someday. Not required though :)
    return "alias" + (aliasCounter++);
  }

  private static ArrayList<HQLContextInterface> getUnionContexts(CandidateFact fact, Map<Dimension, CandidateDim>
    dimsToQuery, CubeQueryContext query)
    throws LensException {
    ArrayList<HQLContextInterface> contexts = new ArrayList<>();
    String alias = query.getAliasForTableName(query.getCube().getName());
    for (String storageTable : fact.getStorageTables()) {
      SingleFactHQLContext ctx = new SingleFactHQLContext(fact, storageTable + " " + alias, dimsToQuery, query,
        fact.getWhereClause(storageTable.substring(storageTable.indexOf(".") + 1)));
      contexts.add(ctx);
    }
    return contexts;
  }
}
