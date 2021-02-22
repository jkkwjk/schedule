package com.jkk.taskpublish.entity;

import com.jkk.Task;
import lombok.Data;

@Data
public class NodeTask {
	private String clientId;
	private Task task;
}
