package com.hadoop.MovieLensKPI3;

public class AgeStringFactory {
	
	public static String getAgeString(String userAge) {
		int age = Integer.parseInt(userAge);
		if (age < 18) {
			return null;
		} else if (age >= 18 && age <= 35) {
			return "18-35";
		} else if (age >= 36 && age <= 50) {
			return "36-50";
		} else if (age > 50) {
			return "50+";
		}
		return null;
	}
}
