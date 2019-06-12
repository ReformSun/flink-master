package com.test.util;

import java.util.Random;

public class RandomUtil {
	private static Random random = new Random();
    public static int getRandom(int size)
    {
        return random.nextInt(size);
    }

	public static int getRandom(int start,int size)
	{
		return start + random.nextInt(size);
	}
	public static int getRandomToEnd(int start,int end)
	{
		return getRandom(start,end - start);
	}

	public static int getRandomHash()
	{
		return random.doubles().hashCode();
	}

	/**
	 * 获取指定字符
	 * @param number 字符ascII码
	 * @return
	 */
	public static String getStringFromInt(int number)
	{
		char[] chars = {(char)(number)};
		return new String(chars);
	}

	public static String getStringFromRandom(int start,int size)
	{
		int number = getRandom(start,size);
		char[] chars = {(char)(number)};
		return new String(chars);
	}


	public static void main(String[] args) {
		for (int i = 0; i < 100; i++) {
//			System.out.println(getRandomHash());
//			System.out.println(getRandom(2));
			System.out.println(getRandom(97,2));
		}
	}
}
