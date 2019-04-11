package com.test;

import java.io.Serializable;

public class TestClass implements Serializable{
	private String s;

	public TestClass(String s) {
		this.s = s;
	}

	public String getS() {
		return s;
	}
}
