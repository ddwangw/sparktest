public class CodeTest {
    private static final String janPattern = "(0?[13578]|1[02])/(0?[1-9]|[12][0-9]|3[01])";
    private static final String febPattern = "0?2/(0?[1-9]|[12][0-9])";
    private static final String aprPattern = "(0?[469]|11)/(0?[1-9]|[12][0-9]|30)";
    private static final String timeFormat = String.format("^2[0-9]{3}/(%s|%s|%s) ([01][0-9]|2[0-3])(:[0-5][0-9]){2}$", febPattern, janPattern, aprPattern);
    public static void main(String[] args){
//        System.out.println("2000/11/1 08:10:00".matches(timeFormat));
        System.out.println("蒙HE6397".substring(0,1));
    }
}
