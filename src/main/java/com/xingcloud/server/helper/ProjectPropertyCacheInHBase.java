package com.xingcloud.server.helper;

import com.xingcloud.userprops_meta_util.UpdateFunc;
import com.xingcloud.userprops_meta_util.UserProp;
import com.xingcloud.userprops_meta_util.UserProps_DEU_Util;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * User: IvyTang
 * Date: 13-7-5
 * Time: PM5:47
 */
public class ProjectPropertyCacheInHBase {
  private static final Log LOG = LogFactory.getLog(ProjectPropertyCacheInHBase.class);

  private static ProjectPropertyCacheInHBase instance = new ProjectPropertyCacheInHBase();

  private static Map<String, Map<String, UserProp>> pid_UserProps = new HashMap<String, Map<String, UserProp>>();

  private ProjectPropertyCacheInHBase() {
    // do nothing
  }

  public static ProjectPropertyCacheInHBase getInstance() {
    return instance;
  }

  public int getPropertyID(String project, String property) {
    int returnID = Constants.NULL_MAXPROPERTYID;
    try {
      Map<String, UserProp> userPropMap = pid_UserProps.get(project);

      if (userPropMap == null)
        userPropMap = resetUserProps(project);
      UserProp userProp = userPropMap.get(property);
      if (userProp != null)
        returnID = userProp.getId();

    } catch (IOException e) {
      LOG.error(project + " get prop id error. " + property);
    }
    return returnID;
  }

  public UpdateFunc getPropertyFunc(String project, String property) {
    return pid_UserProps.get(project).get(property).getPropFunc();
  }

  public static  Map<String, UserProp> resetUserProps(String project) throws IOException {
    LOG.info("enter resetUserProps"+project);
    List<UserProp> userProps = UserProps_DEU_Util.getInstance().getUserProps(project);
    Map<String, UserProp> userPropMap = new HashMap<String, UserProp>();
    for (UserProp userProp : userProps) {
      userPropMap.put(userProp.getPropName(), userProp);    }

    pid_UserProps.put(project, userPropMap);
    return userPropMap;
  }
}
