package com.xingcloud.server.helper;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * User: IvyTang
 * Date: 12-12-3
 * Time: 上午11:48
 */
public class ConfigReader {
    public static final Log LOG = LogFactory.getLog(ConfigReader.class);

    private Map<String,Dom> allConfigFile;

    private ConfigReader() {
        allConfigFile=new HashMap<String,Dom>();
    }
    private Dom readFile (String file){
        Dom dom=null;
        try {
            InputStream is = this.getClass().getClassLoader().getResourceAsStream(file);
            dom=Dom.getRoot(is);
            is.close();
        } catch (Exception e) {
            LOG.error("the config not find "+file,e);
        }
        return dom;
    }
    private Dom getDomRoot(String file){
        Dom dom=allConfigFile.get(file);
        if(dom==null) {
            dom=readFile(file);
            if(dom!=null)allConfigFile.put(file,dom);
        }
        return dom;
    }
    private String getConfigField(String file, String[] path){
        Dom configFile=getDomRoot(file);

        if(configFile==null) return null;
        else{
            Dom now=configFile;
            for(String anPath:path){
                if(now.existElement(anPath)){
                    now=now.element(anPath);
                }
                else return null;
            }
            return now.getSelfText();
        }
    }
    static private ConfigReader instance;
    /**
     * get the single instance;
     * @return reutrn instance;
     */
    static private ConfigReader getInstance(){
        if(instance==null) instance=new ConfigReader();
        return instance;
    }


    /**
     * get the config field in the config file
     * @param file the config file
     * @param path  the config key from the root to the son
     *          the key should link the path with *
     * @return the config or null
     */
    static public String getConfig(String file,String ... path){

        return ConfigReader.getInstance().getConfigField(file, path);
    }

    /**
     * get the config file 's dom root
     * @param file  the file path
     * @return  the file 's dom root
     */
    static public Dom getDom(String file){
        return ConfigReader.getInstance().getDomRoot(file);
    }

    static public List<Dom> getDomList(String file,String ...path){
        Dom root=ConfigReader.getInstance().getDomRoot(file);
        if(path!=null){
            int i;
            for(i=0;i<path.length-1;i++){
                root=root.element(path[i]);
            }
            return root.elements(path[i]);
        }
        return null;
    }

    static public List<Dom> getDomList(Dom root,String ...path){
        Dom temp=root;
        if(path!=null){
            int i;
            for(i=0;i<path.length-1;i++){
                temp=temp.element(path[i]);
            }
            return temp.elements(path[i]);
        }
        return null;
    }
}
