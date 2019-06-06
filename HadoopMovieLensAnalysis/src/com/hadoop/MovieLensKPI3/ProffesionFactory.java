package com.hadoop.MovieLensKPI3;

public class ProffesionFactory {
	
	public static String getAgeString(String userAge) {
		int occupationCode = Integer.parseInt(userAge);
		if (occupationCode == 0) {
			return "other or not specified";
		} else if (occupationCode ==1) {
			return "academic/educator";
		} else if (occupationCode ==2) {
			return "artist";
		} else if (occupationCode ==3) {
			return "clerical/admin";
		} else if (occupationCode ==4) {
			return "college/grad student";
		} else if (occupationCode ==5) {
			return "customer service";
		} else if (occupationCode ==6) {
			return "doctor/health care";
		} else if (occupationCode ==7) {
			return "executive/managerial";
		} else if (occupationCode ==8) {
			return "farmer";
		} else if (occupationCode ==9){
			return "homemaker";
		} else if (occupationCode ==10) {
			return "K-12 student";
		} else if (occupationCode ==11) {
			return "lawyer";
		} else if (occupationCode ==12) {
			return "programmer";
		} else if (occupationCode ==13) {
			return "retired";
		} else if (occupationCode ==14) {
			return "sales/marketing";
		} else if (occupationCode ==15) {
			return "scientist";
		} else if (occupationCode ==16) {
			return "self-employed";
		} else if (occupationCode ==17) {
			return "technician/engineer";
		} else if (occupationCode ==18) {
			return "tradesman/craftsman";
		} else if (occupationCode ==19) {
			return "unemployed";
		} else if (occupationCode ==20) {
			return "writer";
		}	else {
		return null;
	}
}
}