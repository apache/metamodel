package org.apache.metamodel.create;
 
import java.util.HashMap;
import java.util.Map;
 
enum ContextType {
 
    JdbcDataContext {
        void updateCharMap() {
            charMap.put(" ", "_");
            charMap.put(";", "_");
        }
    },
    CsvDataContext {
        void updateCharMap() {
            charMap.put(";","");
        }
    },
    MockUpdateableDataContext {
    	void updateCharMap() {
    		charMap.put(" ", "_");
    		charMap.put("'", "");
    	}
    };
  
    Map<String,String> charMap = new HashMap<String,String>();
  
    private ContextType() {
        updateCharMap();
    }
  
    public Map<String,String> getCharMap() {
        return new HashMap<String,String>(charMap);
    }
  
    public static ContextType getContext(String contextName) {
        try {
            return ContextType.valueOf(contextName);
        } catch(IllegalArgumentException exp) {
             return null;
        }
    }
  
    abstract void updateCharMap();
}
