package com.hadoop.MovieLensKPI3;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UserDataMapper  extends Mapper<Object,Text,Text,Text>{

	private Text userId = new Text();
	private Text outvalue = new Text();
	private HashMap<Integer, String> profession_mapping = new HashMap<Integer, String>();
	private String profession_id_file_location = null;

	@Override
	public void map(Object Key, Text value, Context context) throws IOException, InterruptedException {

		//	UserID::Gender::Age::Occupation::Zip-code
		//	1::F::1::10::48067
		String line = value.toString();
		String[] lineparts = line.split("::");	
		if(lineparts.length>1) {
			String sUserId = lineparts[0];
			userId.set(sUserId);
			String sProffesion = ProffesionFactory.getAgeString(lineparts[3]);
			String sAgeGroup = AgeStringFactory.getAgeString(lineparts[2]);
			outvalue.set("U"+sAgeGroup+"::"+sProffesion);
			context.write(userId,outvalue);
		}
	}	
	@Override
	public void setup(Context context) {
		profession_id_file_location = context.getConfiguration().get("id.to.profession.mapping.file.path");
		try {
			BufferedReader bufferedReader = new BufferedReader(new FileReader(profession_id_file_location));
			String line;
			while ((line = bufferedReader.readLine()) != null) {
				String[] field = line.split(",", -1);
				profession_mapping.put(Integer.parseInt(field[0]), field[1]);
			}
			bufferedReader.close();
		} catch (Exception ex) {
			System.out.println(ex.getLocalizedMessage());
		}
	}

}