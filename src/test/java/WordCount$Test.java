import org.junit.Test;

import static org.junit.Assert.*;

public class WordCount$Test {

    @Test
    public void debug() throws Exception {
        String[] input = new String[2];

        /*
        1. put the data.txt into a folder in your pc
        2. add the path for the following two files.
            windows : update the path like "file:///C:/Users/.../projectDirectory/data.txt"
            mac or linux: update the path like "file:///Users/.../projectDirectory/data.txt"
        */

        System.setProperty("hadoop.home.dir", "C:\\winutils\\");
        input[0] = "file:///C:/Users/Alex/IdeaProjects/WordCount/data.txt";
        input[1] = "file:///C:/Users/Alex/IdeaProjects/CS4433_Project_2/Test.txt";

        WordCount wc = new WordCount();
        wc.debug(input);
    }
}
