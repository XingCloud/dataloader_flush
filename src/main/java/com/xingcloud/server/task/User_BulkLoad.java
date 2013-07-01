package com.xingcloud.server.task;

import com.xingcloud.mysql.UserProp;
import com.xingcloud.server.helper.ProjectPropertyCache;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.List;

/**
 * User: IvyTang
 * Date: 13-5-13
 * Time: 上午9:53
 */
public class User_BulkLoad {
  private static Log LOG = LogFactory.getLog(User.class);

  private String project;

  private long seqUid;

  private long samplingUid;

  private List<String> propKeys;

  private List<String> propValues;


  public User_BulkLoad(String project, long seqUid, long samplingUid, List<String> propKeys, List<String> propValues) {
    this.project = project;
    this.seqUid = seqUid;
    this.samplingUid = samplingUid;
    this.propKeys = propKeys;
    this.propValues = propValues;
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


  public List<String> getPropKeys() {
    return propKeys;
  }

  public List<String> getPropValues() {
    return propValues;
  }

  @Override
  public String toString() {
    String tmp = project + "\t" + String.valueOf(seqUid) + "\t" + String.valueOf(samplingUid);
    for (int i = 0; i < propKeys.size(); i++) {
      tmp += "\t";
      tmp += propKeys.get(i);
      tmp += propValues.get(i);
    }
    return tmp;


  }
}
