package com.test.util;

import java.util.Random;

public class RandomUtil {
    public static int getRandom(int size)
    {
        Random random = new Random();
        return random.nextInt(size);
    }
}
