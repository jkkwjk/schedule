package com.jkk.taskpublish;


import com.jkk.Task;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

@Getter
@EqualsAndHashCode
public class TaskNodes {
	private final Task task;

	@EqualsAndHashCode.Exclude
	private final List<String> nodes;

	public TaskNodes(Task task, String clientId) {
		this(task);
		nodes.add(clientId);
	}

	public TaskNodes(Task task) {
		this.task = task;
		nodes = new ArrayList<>();
	}

	public void addNodes(String clientId) {
		nodes.add(clientId);
	}

	public Boolean existNodes(String clientId) {
		return nodes.contains(clientId);
	}

	public Boolean removeNodes(String clientId) {
		return nodes.remove(clientId);
	}

}
