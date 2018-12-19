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

	@Override
	public String toString() {
		return  "a=" + a + ";b=" + b + ";c=" + c;
	}

	@Override
	public boolean equals(Object obj) {
		return true;
	}

	@Override
	public int hashCode() {
		return super.hashCode();
	}
}
