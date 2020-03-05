package videoInfoJobTop10;


import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtils {

    private static SimpleDateFormat sdf1= new SimpleDateFormat("yyyyMMdd");
    private static SimpleDateFormat sdf2= new SimpleDateFormat("yyyy-MM-dd");

    /**
     * transform date from yyyymmdd format to yyyy-mm-dd format
     * @param dt
     * @return
     */
    public static String transDataFormat(String dt){
        String res = "1970-01-01";
        try{
            Date date = sdf1.parse(dt);
            res = sdf2.format(date);
        }catch(Exception e){
            System.out.println("failure parse date" + dt);
        }
        return res;
    }
}
