package com.xingcloud.server.helper;


import com.xingcloud.mysql.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * User: IvyTang
 * Date: 12-11-29
 * Time: 上午10:31
 */
public class ProjectPropertyCache {

  public static final Log LOG = LogFactory.getLog(ProjectPropertyCache.class);

  private static final int MS_WHEN_SQL_EXCEPTION = 5 * 1000;

  private String name = null;
  private List<UserProp> list = null;

  private ProjectPropertyCache(String name, List<UserProp> list) {
    this.name = name;
    this.list = list;
  }

  public UserProp getUserPro(String key) {
    if (list != null) {
        // todo: eliminate list iteration
      for (UserProp userProp : list) {
        if (userProp.getPropName().equals(key)) return userProp;
      }
    }
    return null;
  }

  static Map<String, ProjectPropertyCache> cache = new ConcurrentHashMap<String, ProjectPropertyCache>();

  static public ProjectPropertyCache getProjectPropertyCacheFromProject(String project) {
    ProjectPropertyCache projectPropertyCache = cache.get(project);
    if (projectPropertyCache == null) {
      List<UserProp> userPropList = null;
      int tryTimes = 1;
      boolean successful = false;
      while (!successful) {
        try {
          userPropList = MySql_16seqid.getInstance().getUserProps(project);
          projectPropertyCache = new ProjectPropertyCache(project, userPropList);
          cache.put(project, projectPropertyCache);

          successful = true;
        } catch (SQLException sqlexception) {
          LOG.error("get user properties failed." +
            " project: " + project +
            " retry in " + MS_WHEN_SQL_EXCEPTION * tryTimes / 1000 + " seconds." + sqlexception.getMessage());

          try {
            Thread.sleep(MS_WHEN_SQL_EXCEPTION * tryTimes);
            tryTimes = (tryTimes << 1) & Integer.MAX_VALUE;
          } catch (InterruptedException ie) {
            successful = true;
          }
        }
      }
    }
    return projectPropertyCache;
  }

  static public ProjectPropertyCache resetProjectCache(String project) {
    cache.remove(project);
        return getProjectPropertyCacheFromProject(project);
  }


}


