package idc.cloud.ex2;

import java.io.Serializable;

@SuppressWarnings("serial")
public class StudentData implements Serializable {

	StudentData() {
	}

	public StudentData(int grade, long version) {
		this.grade = grade;
		this.version = version;
	}

	public StudentData(int oldGrade, int grade, long version) {
		this.grade = grade;
		this.oldGrade = oldGrade;
		this.version = version;
	}

	private long version;
	private int grade;
	private Integer oldGrade;

	public int getGrade() {
		return grade;
	}

	public Integer getOldGrade() {
		return oldGrade;
	}

	public long getVersion() {
		return version;
	}

	@Override
	public String toString() {
		return "StudentData [version=" + version + ", grade=" + grade + ", oldGrade=" + oldGrade + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + grade;
		result = prime * result + ((oldGrade == null) ? 0 : oldGrade.hashCode());
		result = prime * result + (int) (version ^ (version >>> 32));
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
		StudentData other = (StudentData) obj;
		if (grade != other.grade)
			return false;
		if (oldGrade == null) {
			if (other.oldGrade != null)
				return false;
		} else if (!oldGrade.equals(other.oldGrade))
			return false;
		if (version != other.version)
			return false;
		return true;
	}

}
