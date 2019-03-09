package com.test.util;

import java.util.Random;

public class RandomUtil {
    public static int getRandom(int size)
    {
        Random random = new Random();
        return random.nextInt(size);
    }

	public static int getRandomHash()
	{
		Random random = new Random();
		return random.doubles().hashCode();
	}

//	public static void main(String[] args) {
//		for (int i = 0; i < 100; i++) {
////			System.out.println(getRandomHash());
//			System.out.println(getRandom(2));
//		}
//	}
}
