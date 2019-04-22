package com.test.util;

import java.util.Random;

public class RandomUtil {
	private static Random random = new Random();
    public static int getRandom(int size)
    {
        return random.nextInt(size);
    }

	public static int getRandom(int start,int end)
	{
		return start + random.nextInt(end);
	}

	public static int getRandomHash()
	{
		return random.doubles().hashCode();
	}


	public static void main(String[] args) {
		for (int i = 0; i < 100; i++) {
//			System.out.println(getRandomHash());
//			System.out.println(getRandom(2));
			System.out.println(getRandom(1,5));
		}
	}
}
