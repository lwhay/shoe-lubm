/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.jdbc;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.service.HiveInterface;
import org.apache.hadoop.hive.service.HiveServerException;
import org.apache.hadoop.io.BytesWritable;
//import cg.demo.SearchDemo;
/**
 * HiveQueryResultSet.
 *
 */
public class HiveQueryResultSet extends HiveBaseResultSet {

  public static final Log LOG = LogFactory.getLog(HiveQueryResultSet.class);

  private HiveInterface client;
  private SerDe serde;

  private boolean isHql;
  private boolean firstGet;
//  public static SearchDemo demo = new SearchDemo("config.txt");

  private int maxRows = 0;
  private int rowsFetched = 0;
  private int fetchSize = 50;

  private List<String> fetchedRows;
  private Iterator<String> fetchedRowsItr;

  private boolean isHql() { return isHql; }

  public HiveQueryResultSet(HiveInterface client, int maxRows, boolean isHql_) throws SQLException {
    this.client = client;
    this.maxRows = maxRows;
    isHql = isHql_;
    firstGet = false;
    initSerde();
    row = Arrays.asList(new Object[columnNames.size()]);
  }

  public HiveQueryResultSet(HiveInterface client) throws SQLException {
    this(client, 0, true);
  }

  private void initSerde1() throws SQLException {
    try {
      Schema fullSchema = client.getSchema();
      List<FieldSchema> schema = fullSchema.getFieldSchemas();
      columnNames = new ArrayList<String>();
      columnTypes = new ArrayList<String>();
      StringBuilder namesSb = new StringBuilder();
      StringBuilder typesSb = new StringBuilder();

      if ((schema != null) && (!schema.isEmpty())) {
        for (int pos = 0; pos < schema.size(); pos++) {
          if (pos != 0) {
            namesSb.append(",");
            typesSb.append(",");
          }
	  String reg = "_col*";
	  Pattern pattern = Pattern.compile(reg);
	  Matcher matcher = pattern.matcher(schema.get(pos).getName());
          //lwh
          columnNames.add(schema.get(pos).getName());
          namesSb.append(schema.get(pos).getName());
	  if (!isHql && matcher.find()) {
	    schema.get(pos).setType(serdeConstants.STRING_TYPE_NAME);
	  }
          columnTypes.add(schema.get(pos).getType());
          typesSb.append(schema.get(pos).getType());
        }
      }
      String names = namesSb.toString();
      String types = typesSb.toString();
      System.out.println(names);
      System.out.println(types);

      serde = new LazySimpleSerDe();
      Properties props = new Properties();
      if (names.length() > 0) {
        LOG.debug("Column names: " + names);
        props.setProperty(serdeConstants.LIST_COLUMNS, names);
      }
      if (types.length() > 0) {
        LOG.debug("Column types: " + types);
        props.setProperty(serdeConstants.LIST_COLUMN_TYPES, types);
      }
      serde.initialize(new Configuration(), props);

    } catch (Exception ex) {
      ex.printStackTrace();
      throw new SQLException("Could not create ResultSet: " + ex.getMessage(), ex);
    }
  }
  /**
   * Instantiate the serde used to deserialize the result rows.
   */
  private void initSerde() throws SQLException {
    try {
      Schema fullSchema = client.getSchema();
      List<FieldSchema> schema = fullSchema.getFieldSchemas();
      columnNames = new ArrayList<String>();
      columnTypes = new ArrayList<String>();
      StringBuilder namesSb = new StringBuilder();
      StringBuilder typesSb = new StringBuilder();

      if ((schema != null) && (!schema.isEmpty())) {
        for (int pos = 0; pos < schema.size(); pos++) {
          if (pos != 0) {
            namesSb.append(",");
            typesSb.append(",");
          }
	  String reg = "_col*";
	  Pattern pattern = Pattern.compile(reg);
	  Matcher matcher = pattern.matcher(schema.get(pos).getName());
          //lwh
          columnNames.add(schema.get(pos).getName());
          namesSb.append(schema.get(pos).getName());
	  if (/*!isHql &&*/ false && matcher.find()) {
	    schema.get(pos).setType(serdeConstants.STRING_TYPE_NAME);
	  }
          columnTypes.add(schema.get(pos).getType());
          typesSb.append(schema.get(pos).getType());
        }
      }
      String names = namesSb.toString();
      String types = typesSb.toString();
      System.out.println(names);
      System.out.println(types);

      serde = new LazySimpleSerDe();
      Properties props = new Properties();
      if (names.length() > 0) {
        LOG.debug("Column names: " + names);
        props.setProperty(serdeConstants.LIST_COLUMNS, names);
      }
      if (types.length() > 0) {
        LOG.debug("Column types: " + types);
        props.setProperty(serdeConstants.LIST_COLUMN_TYPES, types);
      }
      serde.initialize(new Configuration(), props);

    } catch (Exception ex) {
      ex.printStackTrace();
      throw new SQLException("Could not create ResultSet: " + ex.getMessage(), ex);
    }
  }

  @Override
  public void close() throws SQLException {
    client = null;
  }

  /**
   * Moves the cursor down one row from its current position.
   *
   * @see java.sql.ResultSet#next()
   * @throws SQLException
   *           if a database access error occurs.
   */
  public boolean next() throws SQLException {
    if (maxRows > 0 && rowsFetched >= maxRows) {
      return false;
    }
    if (firstGet == false) {
      firstGet = true;
      initSerde1();
    }

    try {
      if (fetchedRows == null || !fetchedRowsItr.hasNext()) {
        fetchedRows = client.fetchN(fetchSize);
        fetchedRowsItr = fetchedRows.iterator();
      }

      String rowStr = "";
      if (fetchedRowsItr.hasNext()) {
        rowStr = fetchedRowsItr.next();
//	System.out.println("-----" + rowStr);
        if (false && !isHql) {
	  String text = rowStr;
	  String test[] = text.split("\t");
	  String newtext = "";
	  for (String te : test) {
	    te.trim();
	    if (te.matches("(\\d+)")) {
	  //    newtext = newtext + demo.searchBTree(Long.valueOf(te)) + "\t";
	    }
            else {
	      System.out.println("Error on sparql split!");
	    }
	  }
          rowStr = newtext;
	}
//	System.out.println("*****" + rowStr);
      } else {
        return false;
      }

      rowsFetched++;
      if (LOG.isDebugEnabled()) {
        LOG.debug("Fetched row string: " + rowStr);
      }

      StructObjectInspector soi = (StructObjectInspector) serde.getObjectInspector();
      List<? extends StructField> fieldRefs = soi.getAllStructFieldRefs();
      Object data = serde.deserialize(new BytesWritable(rowStr.getBytes()));

      assert row.size() == fieldRefs.size() : row.size() + ", " + fieldRefs.size();
      for (int i = 0; i < fieldRefs.size(); i++) {
        StructField fieldRef = fieldRefs.get(i);
        ObjectInspector oi = fieldRef.getFieldObjectInspector();
        Object obj = soi.getStructFieldData(data, fieldRef);
        row.set(i, convertLazyToJava(obj, oi));
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("Deserialized row: " + row);
      }
    } catch (HiveServerException e) {
      if (e.getErrorCode() == 0) { // error code == 0 means reached the EOF
        return false;
      } else {
        throw new SQLException("Error retrieving next row", e);
      }
    } catch (Exception ex) {
      ex.printStackTrace();
      throw new SQLException("Error retrieving next row", ex);
    }
    // NOTE: fetchOne dosn't throw new SQLException("Method not supported").
    return true;
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
    fetchSize = rows;
  }

  @Override
  public int getFetchSize() throws SQLException {
    return fetchSize;
  }

  public <T> T getObject(String columnLabel, Class<T> type)  throws SQLException {
    //JDK 1.7
        throw new SQLException("Method not supported");
  }

  public <T> T getObject(int columnIndex, Class<T> type)  throws SQLException {
    //JDK 1.7
        throw new SQLException("Method not supported");
  }

  /**
   * Convert a LazyObject to a standard Java object in compliance with JDBC 3.0 (see JDBC 3.0
   * Specification, Table B-3: Mapping from JDBC Types to Java Object Types).
   *
   * This method is kept consistent with {@link HiveResultSetMetaData#hiveTypeToSqlType}.
   */
  private static Object convertLazyToJava(Object o, ObjectInspector oi) {
    Object obj = ObjectInspectorUtils.copyToStandardObject(o, oi, ObjectInspectorCopyOption.JAVA);

    // for now, expose non-primitive as a string
    // TODO: expose non-primitive as a structured object while maintaining JDBC compliance
    if (obj != null && oi.getCategory() != ObjectInspector.Category.PRIMITIVE) {
      obj = obj.toString();
    }

    return obj;
  }

}

