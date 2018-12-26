package model;

public class Event {
	private String a;
	private int b;
	private double c;
	private long time;
	private String patternName;
	private int numberOfTimes;

	public void setNumberOfTimes(int numberOfTimes) {
		this.numberOfTimes = numberOfTimes;
	}

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
		if (patternName == null){
			return  "numberOfTimes:" + numberOfTimes + "a=" + a + ";b=" + b + ";c=" + c + ":time=" + time;
		}else {
			return  "numberOfTimes:" + numberOfTimes + "模式：" + patternName + "  a=" + a + ";b=" + b + ";c=" + c + ":time=" + time;
		}
	}

	public void setPatternName(String patternName) {
		this.patternName = patternName;
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
