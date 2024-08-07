package com.gthm.lambda.dynamodb.model;

import lombok.*;



@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Student {
	

	private String id;
	private String name;
	private int age;
	private STATUS status = STATUS.CREATED;

}