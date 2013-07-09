package com.xingcloud.server.task;

import com.xingcloud.mysql.UserProp;
import com.xingcloud.server.helper.ProjectPropertyCache;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import sun.util.LocaleServiceProviderPool;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * User: IvyTang
 * Date: 12-11-28
 * Time: 下午2:35
 */
public class User {

  private static Log LOG = LogFactory.getLog(User.class);

  private String project;

  private long seqUid;

  private long samplingUid;

  private Map<String, Object> properties;

  public User(String project, long seqUid, long samplingUid, Map<String, Object> properties) {
    this.project = project;
    this.seqUid = seqUid;
    this.samplingUid = samplingUid;
    this.properties = properties;
  }


  public List<String> getSqlProperties() {
    List<String> sqls = new ArrayList<String>();
    ProjectPropertyCache projectPropertyCache = ProjectPropertyCache.getProjectPropertyCacheFromProject(project);
    if (projectPropertyCache != null) {
      List<String> sortKeys = new ArrayList<String>(properties.keySet());
      Collections.sort(sortKeys);
      for(String key:sortKeys){
        if (projectPropertyCache.getUserPro(key) == null) {
          projectPropertyCache = ProjectPropertyCache.resetProjectCache(project);
        }
        String tempSql = getSqlWithProperty(samplingUid, projectPropertyCache.getUserPro(key),
                key, properties.get(key));
        if (tempSql != null)
          sqls.add(tempSql);
      }
//            for (Map.Entry<String, Object> entry : properties.entrySet()) {
//                if (projectPropertyCache.getUserPro(entry.getKey()) == null) {
//                    projectPropertyCache = ProjectPropertyCache.resetProjectCache(project);
//                }
//                String tempSql = getSqlWithProperty(samplingUid, projectPropertyCache.getUserPro(entry.getKey()),
//                        entry.getKey(), entry.getValue());
//                if (tempSql != null)
//                    sqls.add(tempSql);
//            }
    }
    return sqls;
  }

  /**
   * 根据不同的更新方法和类型，生成不同的sql语句
   *
   * @param samplingSeqUid 用户的uid
   * @param userProp       属性对象
   * @param key            属性key
   * @param value          属性值
   * @return 更新的sql语句
   */
  public String getSqlWithProperty(long samplingSeqUid, UserProp userProp, String key, Object value) {
    try {
      if (value == null) return null;
      switch (userProp.getPropType()) {
        case sql_datetime:
          Long longValue = 0L;
          if (value instanceof String) longValue = Long.valueOf((String) value);
          else if (value instanceof Long) longValue = (Long) value;
          else return null;

          switch (userProp.getPropFunc()) {
            case once:
              return "insert ignore into " + tableName(key) + "( uid , val ) values ( " + samplingSeqUid + "," + longValue + ");";
            case cover:
              return "replace into " + tableName(key) + "( uid , val ) values ( " + samplingSeqUid + "," + longValue + ");";
          }
          break;
        case sql_bigint:
          Integer integer = 0;
          if (value instanceof String) integer = Integer.valueOf((String) value);
          else integer = Integer.parseInt(value.toString());

          switch (userProp.getPropFunc()) {
            case once:
              break;
            case cover:
              return "replace into " + tableName(key) + "( uid , val ) values ( " + samplingSeqUid + "," + integer + ");";
            case inc:

              return " INSERT INTO " + tableName(key) + " (uid,val) VALUES (" + samplingSeqUid + "," + integer + ") ON DUPLICATE KEY UPDATE val=val+" + integer + ";";
          }
          break;
        case sql_string:
          String string = null;
          if (value instanceof String) string = (String) value;
          else return null;
          if (string == null || string.equals("null")) return null;

          string = judgeLength(string);
          string = string.replace("\"", "\\\"");
          string = StringEscapeUtils.escapeSql(string);
          switch (userProp.getPropFunc()) {
            case once:
              return "insert ignore  into " + tableName(key) + "( uid , val ) values ( " + samplingSeqUid + ",\"" + string + "\");";
            case cover:
              return "replace into " + tableName(key) + "( uid , val ) values ( " + samplingSeqUid + ",\"" + string + "\");";
            case inc:
              break;
          }
          break;
      }
    } catch (Exception e) {
      //可能属性转换失败，直接抛弃
    }
    return null;
  }

  /**
   * cut the string with the length of the field in sql
   * 判断属性值的长度，255。
   *
   * @param temp
   * @return
   */
  private String judgeLength(String temp) {
    if (temp.length() > 255) return temp.substring(0, 255);
    else return temp;
  }

  public String tableName(String key) {
    return "`" + key + "`";
  }

  public long getSeqUid() {
    return seqUid;
  }

  public long getSamplingUid() {
    return samplingUid;
  }

  public Map<String, Object> getProperties() {
    return properties;
  }

  @Override
  public String toString() {

    String tmp = project + "\t" + String.valueOf(seqUid) + "\t" + String.valueOf(samplingUid);
    for (Map.Entry<String, Object> entry : properties.entrySet()) {
      tmp += "\t";
      tmp += entry.getKey();
      tmp += entry.getValue();

    }
    return tmp;


  }
}