package com.jkk.taskpublish.exception;

import com.jkk.Task;

public class OutOfResourcesException extends RuntimeException{
	public OutOfResourcesException(Task task) {
		super(task.toString());
	}
}
