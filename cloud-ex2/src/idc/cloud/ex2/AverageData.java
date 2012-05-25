package idc.cloud.ex2;

import java.io.Serializable;

@SuppressWarnings("serial")
public class AverageData implements Serializable {
	private int count;
	private long sum;

	AverageData() {
	}

	@Override
	public String toString() {
		return "AverageData [count=" + count + ", sum=" + sum + "]";
	}

	public float getAverage() {
		if (count == 0)
			return 0;
		return ((float) sum) / count;

	}

	public int getCount() {
		return count;
	}

	public long getSum() {
		return sum;
	}

	public AverageData(int count, long sum) {
		this.count = count;
		this.sum = sum;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + count;
		result = prime * result + (int) (sum ^ (sum >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		AverageData other = (AverageData) obj;
		if (count != other.count)
			return false;
		if (sum != other.sum)
			return false;
		return true;
	}

}
