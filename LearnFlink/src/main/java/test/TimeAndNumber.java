package test;

public class TimeAndNumber {
	private Long timestamp;
	private Long number;

	public TimeAndNumber(Long timestamp, Long number) {
		this.timestamp = timestamp;
		this.number = number;
	}

	@Override
	public String toString() {
		return "时间：" + timestamp + " 数字：" + number;
	}
}
