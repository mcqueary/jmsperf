package com.tibco.mcqueary.jmsperf;

import java.util.Random;

public class TestUtils {
	public static int getRandomInt(int Low, int High)
	{
		Random r = new Random();
//		int Low = 10;
//		int High = 100;
		int R = r.nextInt(High-Low) + Low;
		return R;
	}
	
}
