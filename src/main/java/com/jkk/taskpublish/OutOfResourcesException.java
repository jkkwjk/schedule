package com.jkk.taskpublish;

import com.jkk.Task;

public class OutOfResourcesException extends RuntimeException{
	public OutOfResourcesException(Task task) {
		super(task.toString());
	}
}
