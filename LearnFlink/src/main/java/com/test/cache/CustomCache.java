package com.test.cache;

import java.util.concurrent.ExecutionException;

public interface CustomCache<T> {
	public Value getValue(T key) throws ExecutionException;
}
