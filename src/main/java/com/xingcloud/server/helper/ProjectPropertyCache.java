package com.xingcloud.server.helper;


import com.xingcloud.mysql.MySql_16seqid;
import com.xingcloud.mysql.UserProp;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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
    private String name = null;
    private List<UserProp> list = null;

    private ProjectPropertyCache(String name, List<UserProp> list) {
        this.name = name;
        this.list = list;
    }

    public UserProp getUserPro(String key) {
        if (list != null) {
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
            List<UserProp> locallist = null;
            try {
                locallist = MySql_16seqid.getInstance().getUserProps(project);
                projectPropertyCache = new ProjectPropertyCache(project, locallist);
            } catch (Exception e) {
                LOG.error("MySql_16seqid getProjectPropertyCacheFromProject " + project, e);
                projectPropertyCache = new ProjectPropertyCache(project, null);
            }
            cache.put(project, projectPropertyCache);
        }
        return projectPropertyCache;
    }

    static public ProjectPropertyCache resetProjectCache(String project) {
        cache.remove(project);
        return getProjectPropertyCacheFromProject(project);

    }

}


