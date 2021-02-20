package com.jkk.taskpublish;

import com.jkk.Task;
import lombok.Data;

@Data
public class NodeTask {
	private String clientId;
	private Task task;
}
