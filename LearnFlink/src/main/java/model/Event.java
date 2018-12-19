package model;

public class Event {
	private String a;
	private int b;
	private double c;
	private long time;

	public String getA() {
		return a;
	}

	public int getB() {
		return b;
	}

	public double getC() {
		return c;
	}

	public long getTime() {
		return time;
	}

	public Event(int b, String a, double c, long time) {
		this.a = a;
		this.b = b;
		this.c = c;
		this.time = time;
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
