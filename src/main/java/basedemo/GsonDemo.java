package basedemo;

/**
 * 功能描述: <br>
 *
 * @since: 1.0.0
 * @Author:wjp
 * @Date: 2020/4/11 21:28
 */
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.util.List;

public class GsonDemo {
    public static void main(String[] args) {
        String strJsonArray = "[\"Java1\",\"Android2\",\"IOS3\"]";
        parseJsonStrToList(strJsonArray);

    }
    /**
     * 数组格式的 Json 字符串 - > 转为 List 列表
     */
    public static void parseJsonStrToList(String str){
        Gson gson = new Gson();
        List<String> strList = gson.fromJson(str, new TypeToken<List<String>>(){}.getType());
        for (int i=0; i<strList.size(); i++){
            System.out.println((i + 1) + ":" + strList.get(i));
        }
    }
    /**
     * 数组格式的 Json 字符串 - > 转为 json 对象
     */
    public static void parseJsonStringToJsonObject(String s) {
        JsonParser jsonParser = new JsonParser();
        JsonObject jsonObject = jsonParser.parse(s).getAsJsonObject();//解析的字符串不符合格式则抛出异常
        
    }

}
