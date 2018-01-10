package data.streaming.dto;

public class KeywordDTO {

	private String key1, key2;
	private Double statistic;

	public KeywordDTO(String key1, String key2,  Double statistic) {
		super();
		this.key1 = key1;
		this.key2 = key2;
		this.statistic = statistic;
	}

	public String getKey1() {
		return key1;
	}

	public void setKey1(String key) {
		this.key1 = key;
	}

	public String getKey2() {
		return key2;
	}

	public void setKey2(String key2) {
		this.key2 = key2;
	}

	public Double getStatistic() {
		return statistic;
	}

	public void setStatistic(Double statistic) {
		this.statistic = statistic;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((key1 == null) ? 0 : key1.hashCode());
		result = prime * result + ((key2 == null) ? 0 : key2.hashCode());
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
		KeywordDTO other = (KeywordDTO) obj;
		if (key1 == null) {
			if (other.key1 != null)
				return false;
		} else if (!key1.equals(other.key1))
			return false;
		if (key2 == null) {
			if (other.key2 != null)
				return false;
		} else if (!key2.equals(other.key2))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "KeywordDTO [key1=" + key1 + ", key2=" + key2 + ", statistic=" + statistic + "]";
	}

	
}
