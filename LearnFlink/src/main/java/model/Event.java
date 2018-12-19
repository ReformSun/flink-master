package model;

public class Event {
	private String a;
	private int b;
	private double c;

	public String getA() {
		return a;
	}

	public int getB() {
		return b;
	}

	public double getC() {
		return c;
	}

	public Event(int b, String a, double c) {
		this.a = a;
		this.b = b;
		this.c = c;
	}
}
