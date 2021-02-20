package com.jkk;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode()
public class Task {
	@EqualsAndHashCode.Exclude
	private String processId;
	private String jarId;
	private String topic;

	public Boolean canReUsingTask(Task task) {
		return task.getJarId().equals(this.jarId);
	}
}
