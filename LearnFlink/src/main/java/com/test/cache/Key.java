package com.test.cache;


import java.io.Serializable;

public class Key implements Serializable {
	private String keyName;
	private String typeInformation;

	public Key(String keyName, String typeInformation) {
		this.keyName = keyName;
		this.typeInformation = typeInformation;
	}

	public String getKeyName() {
		return keyName;
	}

	public String getTypeInformation() {
		return typeInformation;
	}
}
