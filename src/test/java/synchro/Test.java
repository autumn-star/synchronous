package synchro;

import org.junit.runner.RunWith;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
public class Test {

	public static void main(String[] args) {
		String t = "sadf-\nasdfasdf-\r";
		System.out.println(t);
		System.out.println(t.replaceAll("[\\n\\r]", ""));
	}

}
