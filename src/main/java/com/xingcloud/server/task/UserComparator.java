package com.xingcloud.server.task;

import java.util.Comparator;

/**
 * User: IvyTang
 * Date: 13-6-9
 * Time: 下午9:35
 */
public class UserComparator implements Comparator<User_BulkLoad> {


  @Override
  public int compare(User_BulkLoad user_bulkLoad, User_BulkLoad user_bulkLoad2) {
    // todo: what about returning zero?
    if (user_bulkLoad.getSamplingUid() < user_bulkLoad2.getSamplingUid())
      return -1;
    else return 1;
  }
}
