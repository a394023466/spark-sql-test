import org.apache.log4j.Logger;

public class LogTest {
    private static Logger logger = Logger.getLogger(LogTest.class.getName());

    public static void main(String[] args) throws Exception {
        int index = 0;
        while (true) {
            logger.info("value : " + index++);
            Thread.sleep(1000);
        }
    }
}
