package com.xingcloud.server.task;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * User: IvyTang
 * Date: 13-4-12
 * Time: 下午5:03
 */
public class HotUserProjectBitmap {

  private final Set<String> permittedProjects = new HashSet<String>();

  private final Map<String, Bitmap> bitMaps = new HashMap<String, Bitmap>();

  private static HotUserProjectBitmap instance = new HotUserProjectBitmap();

  public static HotUserProjectBitmap getInstance() {
    return instance;
  }

  private HotUserProjectBitmap() {
    permittedProjects.add("age");
    permittedProjects.add("ddt");
    permittedProjects.add("ddt-ff");
    permittedProjects.add("minigames337");
    permittedProjects.add("portaldosites");
    permittedProjects.add("sof-dp");
    permittedProjects.add("qvo6");
    permittedProjects.add("sof-dsk");
    permittedProjects.add("sof-newgdp");
    permittedProjects.add("happyfarmer");
    permittedProjects.add("v9-v9");
    permittedProjects.add("v9nuser");
    permittedProjects.add("22find");
    permittedProjects.add("govome");
    for (String pPid : permittedProjects)
      bitMaps.put(pPid, new Bitmap());
  }

  public boolean ifInLocalCacheHot(String pid, long uid) {
    Bitmap bitmap = bitMaps.get(pid);
    return bitmap != null && bitmap.get(uid);
  }

  public void markLocalCacheHot(String pid, long uid) {
    Bitmap bitmap = bitMaps.get(pid);
    if (bitmap == null)
      return;
    bitmap.set(uid, true);
  }

  public void resetCacheBitmap(String pid) {
    Bitmap bitmap = bitMaps.get(pid);
    if (bitmap == null)
      return;
    bitmap.reset();
  }


  public static void main(String[] args) {

    System.out.println(getInstance().ifInLocalCacheHot("citylife", 23l));
    getInstance().markLocalCacheHot("citylife", 23l);
    System.out.println(getInstance().ifInLocalCacheHot("citylife", 23l));
    getInstance().resetCacheBitmap("citylife");
    System.out.println(getInstance().ifInLocalCacheHot("citylife", 23l));
    getInstance().markLocalCacheHot("citylife", 23l);
    System.out.println(getInstance().ifInLocalCacheHot("citylife", 23l));


  }

}
