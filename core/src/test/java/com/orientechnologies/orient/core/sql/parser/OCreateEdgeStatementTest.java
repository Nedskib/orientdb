package com.orientechnologies.orient.core.sql.parser;

import static org.testng.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import org.testng.annotations.Test;

@Test
public class OCreateEdgeStatementTest {

  protected SimpleNode checkRightSyntax(String query) {
    return checkSyntax(query, true);
  }

  protected SimpleNode checkWrongSyntax(String query) {
    return checkSyntax(query, false);
  }

  protected SimpleNode checkSyntax(String query, boolean isCorrect) {
    OrientSql osql = getParserFor(query);
    try {
      SimpleNode result = osql.OrientGrammar();
      if (!isCorrect) {
        fail();
      }
      return result;
    } catch (Exception e) {
      if (isCorrect) {
        e.printStackTrace();
        fail();
      }
    }
    return null;
  }

  public void testSimpleCreate() {
    checkRightSyntax("create edge Foo from (Select from a) to (Select from b)");
  }


  public void testCreateFromRid() {
    checkRightSyntax("create edge Foo from #11:0 to #11:1");

  }

  public void testCreateFromRidArray() {
    checkRightSyntax("create edge Foo from [#11:0, #11:3] to [#11:1, #12:0]");

  }

  public void testRetry() {
    checkRightSyntax("create edge Foo from [#11:0, #11:3] to [#11:1, #12:0] retry 3 wait 20");

  }


  private void printTree(String s) {
    OrientSql osql = getParserFor(s);
    try {
      SimpleNode n = osql.OrientGrammar();
      n.dump(" ");
    } catch (ParseException e) {
      e.printStackTrace();
    }

  }

  protected OrientSql getParserFor(String string) {
    InputStream is = new ByteArrayInputStream(string.getBytes());
    OrientSql osql = new OrientSql(is);
    return osql;
  }
}
