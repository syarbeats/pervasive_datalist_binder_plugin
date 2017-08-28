package com.itasoft.poc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.sql.DataSource;
import oracle.sql.TIMESTAMP;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.dbcp.BasicDataSourceFactory;
import org.apache.commons.lang.StringEscapeUtils;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.function.SQLFunction;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.type.StandardBasicTypes;
import org.joget.apps.app.model.AppDefinition;
import org.joget.apps.app.service.AppUtil;
import org.joget.apps.datalist.model.DataList;
import org.joget.apps.datalist.model.DataListBinderDefault;
import org.joget.apps.datalist.model.DataListCollection;
import org.joget.apps.datalist.model.DataListColumn;
import org.joget.apps.datalist.model.DataListFilterQueryObject;
import org.joget.apps.userview.model.Userview;
import org.joget.commons.util.DynamicDataSourceManager;
import org.joget.commons.util.LogUtil;
import org.joget.commons.util.ResourceBundleUtil;
import org.joget.commons.util.SecurityUtil;
import org.joget.commons.util.UuidGenerator;
import org.joget.plugin.base.PluginWebSupport;
import org.joget.workflow.util.WorkflowUtil;
import org.json.JSONObject;
import org.springframework.context.ApplicationContext;
import org.springframework.orm.hibernate4.LocalSessionFactoryBean;

public class JdbcPlugin
  extends DataListBinderDefault
  implements PluginWebSupport
{
  public static int MAXROWS = 10000;
  public static String ALIAS = "temp";
  private DataListColumn[] columns;
  
  public String getName()
  {
    return "POC JDBC Binder Pervasive";
  }
  
  public String getVersion()
  {
    return "5.0.0";
  }
  
  public String getDescription()
  {
    return "POC JDBC Binder Pervasive";
  }
  
  public String getLabel()
  {
    return "POC JDBC Binder Pervasive";
  }
  
  public String getPropertyOptions()
  {
	  
      String json = AppUtil.readPluginResource(getClass().getName(), "/userview/pervasive.json", null, true, "message/datalist/jdbcDataListBinder");
	  //String json = AppUtil.readPluginResource(getClass().getName(), "/properties/datalist/jdbcDataListBinderPervasive1.json", null, true, "message/datalist/jdbcDataListBinder");
	    
	  return json;
  }
  
  public DataListColumn[] getColumns()
  {
    if (this.columns == null) {
      try
      {
        String sql = getQuerySelect(null, getProperties(), null, null, null, Integer.valueOf(0), Integer.valueOf(1));
        DataSource ds = createDataSource();
        this.columns = queryMetaData(ds, sql);
      }
      catch (Exception ex)
      {
        LogUtil.error(JdbcPlugin.class.getName(), ex, "");
        throw new RuntimeException(ex.toString());
      }
    }
    return this.columns;
  }
  
  public String getPrimaryKeyColumnName()
  {
    String primaryKey = "";
    Map props = getProperties();
    if (props != null) {
      primaryKey = props.get("primaryKey").toString();
    }
    return primaryKey;
  }
  
  public DataListCollection getData(DataList dataList, Map properties, DataListFilterQueryObject[] filterQueryObjects, String sort, Boolean desc, Integer start, Integer rows)
  {
    try
    {
      DataSource ds = createDataSource();
      
      DataListFilterQueryObject filter = processFilterQueryObjects(ds, filterQueryObjects);
      
     
      String sql = getQuerySelect(dataList, properties, filter, sort, desc, start, rows);
      
      System.out.println("SQL: "+sql);
      
      return executeQuery(dataList, ds, sql, filter.getValues(), start, rows);
    }
    catch (Exception ex)
    {
      LogUtil.error(JdbcPlugin.class.getName(), ex, "");
    }
    return null;
  }
  
  public int getDataTotalRowCount(DataList dataList, Map properties, DataListFilterQueryObject[] filterQueryObjects)
  {
    try
    {
      DataSource ds = createDataSource();
      
      DataListFilterQueryObject filter = processFilterQueryObjects(ds, filterQueryObjects);
      
      String sqlCount = getQueryCount(dataList, properties, filter);
      
      return executeQueryCount(dataList, ds, sqlCount, filter.getValues());
    }
    catch (Exception ex)
    {
      LogUtil.error(JdbcPlugin.class.getName(), ex, "");
    }
    return 0;
  }
  
  protected DataSource createDataSource()
    throws Exception
  {
    Map binderProps = getProperties();
    DataSource ds = null;
    String datasource = (String)binderProps.get("jdbcDatasource");
    if ((datasource != null) && ("default".equals(datasource)))
    {
      ds = (DataSource)AppUtil.getApplicationContext().getBean("setupDataSource");
    }
    else
    {
      Properties dsProps = new Properties();
      dsProps.put("driverClassName", binderProps.get("jdbcDriver").toString());
      dsProps.put("url", binderProps.get("jdbcUrl").toString());
      dsProps.put("username", binderProps.get("jdbcUser").toString());
      dsProps.put("password", binderProps.get("jdbcPassword").toString());
      ds = BasicDataSourceFactory.createDataSource(dsProps);
    }
    return ds;
  }
  
  protected DataListColumn[] queryMetaData(DataSource ds, String sql)
    throws SQLException
  {
    Connection con = null;
    PreparedStatement pstmt = null;
    Collection<DataListColumn> columns = new ArrayList();
    DataListColumn[] columnArray;
    
    try
    {
      con = ds.getConnection();
      pstmt = con.prepareStatement(sql);
      
      String driver = getPropertyString("jdbcDriver");
      String datasource = getPropertyString("jdbcDatasource");
      if ((datasource != null) && ("default".equals(datasource)))
      {
        Properties properties = DynamicDataSourceManager.getProperties();
        driver = properties.getProperty("workflowDriver");
      }
      if ("oracle.jdbc.driver.OracleDriver".equals(driver))
      {
        pstmt.setMaxRows(1);
        pstmt.executeQuery();
      }
      ResultSetMetaData metaData = pstmt.getMetaData();
      int columnCount = metaData.getColumnCount();
      for (int i = 1; i <= columnCount; i++)
      {
        String name = metaData.getColumnName(i);
        String label = metaData.getColumnLabel(i);
        String type = metaData.getColumnTypeName(i);
        boolean sortable = true;
        DataListColumn column = new DataListColumn(name, label, sortable);
        column.setType(type);
        columns.add(column);
      }
      try
      {
        if (pstmt != null) {
          pstmt.close();
        }
      }
      catch (Exception localException) {}
      try
      {
        if (con != null) {
          con.close();
        }
      }
      catch (Exception localException1) {}
      columnArray = (DataListColumn[])columns.toArray(new DataListColumn[0]);
    }
    finally
    {
      try
      {
        if (pstmt != null) {
          pstmt.close();
        }
      }
      catch (Exception localException2) {}
      try
      {
        if (con != null) {
          con.close();
        }
      }
      catch (Exception localException3) {}
    }
    
    return columnArray;
  }
  
  protected String getQuerySelect(DataList dataList, Map properties, DataListFilterQueryObject filterQueryObject, String sort, Boolean desc, Integer start, Integer rows)
  {
    String sql = properties.get("sql").toString();
    sql = "SELECT * FROM (" + sql + ") " + ALIAS;
    if (filterQueryObject != null) {
      sql = insertQueryCriteria(sql, properties, filterQueryObject);
    }
    sql = insertQueryOrdering(sql, sort, desc);
    return sql;
  }
  
  protected String getQueryCount(DataList dataList, Map properties, DataListFilterQueryObject filterQueryObject)
  {
	  
	  
    String sql = properties.get("sql").toString();
    
    sql = "SELECT COUNT(*) FROM (" + sql + ") " + ALIAS;
    sql = insertQueryCriteria(sql, properties, filterQueryObject);
    
    
    return sql;
  }
  
  protected String insertQueryCriteria(String sql, Map properties, DataListFilterQueryObject filterQueryObject)
  {
    if ((sql != null) && (sql.trim().length() > 0))
    {
      String keyName = (String)properties.get(Userview.USERVIEW_KEY_NAME);
      String keyValue = (String)properties.get(Userview.USERVIEW_KEY_VALUE);
      String extra = "";
      if ((filterQueryObject != null) && (filterQueryObject.getQuery() != null) && (filterQueryObject.getQuery().trim().length() > 0)) {
        extra = filterQueryObject.getQuery();
      }
      if (sql.contains("#userviewKey#"))
      {
        if (keyValue == null) {
          keyValue = "";
        }
        sql = sql.replaceAll("#userviewKey#", keyValue);
      }
      else if ((keyName != null) && (!keyName.isEmpty()) && (keyValue != null) && (!keyValue.isEmpty()))
      {
        if (extra.trim().length() > 0) {
          extra = extra + "AND ";
        }
        extra = extra + getName(keyName) + " = '" + keyValue + "' ";
      }
      if ((extra != null) && (!extra.isEmpty())) {
        sql = sql + " WHERE " + extra;
      }
    }
    return sql;
  }
  
  protected String insertQueryOrdering(String sql, String sort, Boolean desc)
  {
    if ((sql != null) && (sql.trim().length() > 0)) {
      if ((sort != null) && (sort.trim().length() > 0))
      {
        String clause = " " + getName(sort);
        if ((desc != null) && (desc.booleanValue())) {
          clause = clause + " DESC";
        }
        sql = sql + " ORDER BY " + clause;
      }
    }
    return sql;
  }
  
  protected DataListCollection executeQuery(DataList dataList, DataSource ds, String sql, String[] values, Integer start, Integer rows)
    throws SQLException
  {
    Connection con = null;
    PreparedStatement pstmt = null;
    ResultSet rs = null;
    DataListCollection results = new DataListCollection();
    
    System.out.println("SQL When its execute: "+sql);
    //System.out.println("Value Length: "+values.length);
    
    if(values != null){
    	
    	/*String firstQuery = sql.substring(0,49);
    	String secondQuery = sql.substring(49);
    	String thirdQuery = secondQuery.substring(6);
    	String[] tempQuery = thirdQuery.split(" ");
    	String subQuery1 = tempQuery[0].substring(1, tempQuery[0].length()-1);
    	String subQuery2 = tempQuery[1];
    	String subQuery3 = tempQuery[2].replace(tempQuery[2], "?");
    	sql = firstQuery +" "+subQuery1+" "+subQuery2+" "+subQuery3;*/
    	
    	String[] query = sql.split("WHERE");
		String secondQuery =  query[1];
		String[] subQuery =  secondQuery.split(" ");
		String firstQuery = query[0];
		String subQuery1 = subQuery[1].substring(6,subQuery[1].length()-1);
		String subQuery2 = subQuery[2];
		String subQuery3 = subQuery[3].replace(subQuery[3], "?");
		sql = firstQuery +" WHERE "+subQuery1+" "+subQuery2+" "+subQuery3;
    	
    	System.out.println("Final SQL:"+sql);
    }
    
     
	
    try
    {
      con = ds.getConnection();
      
      
      /**************************************************************/
      
        
      
		//System.out.println("Final SQL: "+finalQuery);
		
      /**************************************************************/
      
      
      pstmt = con.prepareStatement(sql);
      if ((start == null) || (start.intValue() < 0)) {
        start = Integer.valueOf(0);
      }
      if ((rows != null) && (rows.intValue() != -1))
      {
        int totalRowsToQuery = start.intValue() + rows.intValue();
        pstmt.setMaxRows(totalRowsToQuery);
      }
      if ((values != null) && (values.length > 0)) {
        for (int i = 0; i < values.length; i++) {
          
        	System.out.println("Keyword ke-"+i+":"+values[i]);
        	
        	pstmt.setObject(i + 1, values[i]);
        }
      }
      rs = pstmt.executeQuery();
      DataListColumn[] columns = getColumns();
      int count = 0;
      while (rs.next())
      {
        Map<String, String> row = new HashMap();
        if (count++ >= start.intValue())
        {
          if (columns != null) {
            for (DataListColumn column : columns)
            {
              String columnName = column.getName();
              Object obj = rs.getObject(columnName);
              String columnValue = obj != null ? obj.toString() : "";
              if ((obj instanceof TIMESTAMP))
              {
                TIMESTAMP timestamp = (TIMESTAMP)obj;
                columnValue = timestamp.stringValue();
              }
              row.put(columnName, columnValue);
              row.put(columnName.toLowerCase(), columnValue);
            }
          }
          results.add(row);
        }
      }
      return results;
    }
    finally
    {
      try
      {
        if (rs != null) {
          rs.close();
        }
      }
      catch (Exception localException3) {}
      try
      {
        if (pstmt != null) {
          pstmt.close();
        }
      }
      catch (Exception localException4) {}
      try
      {
        if (con != null) {
          con.close();
        }
      }
      catch (Exception localException5) {}
    }
  }
  
  protected int executeQueryCount(DataList dataList, DataSource ds, String sql, String[] values)
    throws SQLException
  {
    Connection con = null;
    PreparedStatement pstmt = null;
    ResultSet rs = null;
    int count = -1;
    if ((sql != null) && (sql.trim().length() > 0)) {
      try
      {
        con = ds.getConnection();
        
        /*************************************************************/
        
        if(values != null){
        	
        	String[] query = sql.split("WHERE");
    		String secondQuery =  query[1];
    		String[] subQuery =  secondQuery.split(" ");
    		String firstQuery = query[0];
    		String subQuery1 = subQuery[1].substring(6,subQuery[1].length()-1);
    		String subQuery2 = subQuery[2];
    		String subQuery3 = subQuery[3].replace(subQuery[3], "?");
    		sql = firstQuery +" WHERE "+subQuery1+" "+subQuery2+" "+subQuery3;
        	
        	System.out.println("Final SQL (Count):"+sql);
        }
        
        /*************************************************************/
        
        pstmt = con.prepareStatement(sql);
        if ((values != null) && (values.length > 0)) {
          for (int i = 0; i < values.length; i++) {
            pstmt.setObject(i + 1, values[i]);
          }
        }
        rs = pstmt.executeQuery();
        if (rs.next()) {}
        return rs.getInt(1);
      }
      finally
      {
        try
        {
          if (rs != null) {
            rs.close();
          }
        }
        catch (Exception localException3) {}
        try
        {
          if (pstmt != null) {
            pstmt.close();
          }
        }
        catch (Exception localException4) {}
        try
        {
          if (con != null) {
            con.close();
          }
        }
        catch (Exception localException5) {}
      }
    }
	return count;
  }
  
  protected DataListFilterQueryObject processFilterQueryObjects(DataSource ds, DataListFilterQueryObject[] filterQueryObjects)
  {
    for (DataListFilterQueryObject o : filterQueryObjects)
    {
      String query = o.getQuery();
      o.setQuery(query);
    }
    return processFilterQueryObjects(filterQueryObjects);
  }
  
  
  public String getClassName()
  {
    return getClass().getName();
  }
  
  public String getColumnName(String name)
  {
    if (("dateCreated".equalsIgnoreCase(name)) || ("dateModified".equalsIgnoreCase(name)))
    {
      name = getName(name);
      
      String driver = getPropertyString("jdbcDriver");
      String datasource = getPropertyString("jdbcDatasource");
      if ((datasource != null) && ("default".equals(datasource))) {
        try
        {
          DataSource ds = (DataSource)AppUtil.getApplicationContext().getBean("setupDataSource");
          driver = BeanUtils.getProperty(ds, "driverClassName");
        }
        catch (Exception localException) {}
      }
      if (driver.equals("com.mysql.jdbc.Driver")) {
        name = "DATE_FORMAT(" + name + ", '%Y-%m-%d %T')";
      } else if ((driver.equals("oracle.jdbc.driver.OracleDriver")) || (driver.equals("org.postgresql.Driver"))) {
        name = "TO_CHAR(" + name + ", 'YYYY-MM-DD HH24:MI:SS')";
      } else {
        name = "cast(" + name + " as string)";
      }
    }
    return name;
  }
  
  protected String getName(String name)
  {
    if ((name != null) && (!name.isEmpty()))
    {
      DataListColumn[] columns = getColumns();
      for (DataListColumn column : columns) {
        if (name.equalsIgnoreCase(column.getName()))
        {
          name = column.getName();
          break;
        }
      }
      if (name.contains(" ")) {
        name = ALIAS + ".`" + name + "`";
      } else {
        name = ALIAS + '.' + name;
      }
    }
    return name;
  }
  
  public void webService(HttpServletRequest request, HttpServletResponse response)
    throws ServletException, IOException
  {
    boolean isAdmin = WorkflowUtil.isCurrentUserInRole("ROLE_ADMIN");
    if (!isAdmin)
    {
      response.sendError(401);
      return;
    }
    String action = request.getParameter("action");
    String message;
    if ("testConnection".equals(action))
    {
      message = "";
      Connection conn = null;
      try
      {
        AppDefinition appDef = AppUtil.getCurrentAppDefinition();
        
        String jdbcDriver = AppUtil.processHashVariable(request.getParameter("jdbcDriver"), null, null, null, appDef);
        String jdbcUrl = AppUtil.processHashVariable(request.getParameter("jdbcUrl"), null, null, null, appDef);
        String jdbcUser = AppUtil.processHashVariable(request.getParameter("jdbcUser"), null, null, null, appDef);
        String jdbcPassword = AppUtil.processHashVariable(SecurityUtil.decrypt(request.getParameter("jdbcPassword")), null, null, null, appDef);
        
        Class.forName(jdbcDriver);
        conn = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword);
        
        message = ResourceBundleUtil.getMessage("datalist.jdbcDataListBinder.connectionOk");
        try
        {
          if (conn != null) {
            conn.close();
          }
        }
        catch (Exception e)
        {
          LogUtil.error(DynamicDataSourceManager.class.getName(), e, "");
        }
        try
        {
          JSONObject jsonObject = new JSONObject();
          jsonObject.accumulate("message", message);
          jsonObject.write(response.getWriter());
        }
        catch (Exception localException1) {}
      }
      catch (Exception e)
      {
        LogUtil.error(getClassName(), e, "Test Connection error");
        message = ResourceBundleUtil.getMessage("datalist.jdbcDataListBinder.connectionFail") + "\n" + StringEscapeUtils.escapeJavaScript(e.getMessage());
      }
      finally
      {
        try
        {
          if (conn != null) {
            conn.close();
          }
        }
        catch (Exception e)
        {
          LogUtil.error(DynamicDataSourceManager.class.getName(), e, "");
        }
      }
    }
    else
    {
      response.setStatus(204);
    }
  }
}


 