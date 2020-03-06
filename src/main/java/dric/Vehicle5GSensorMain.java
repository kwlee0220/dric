package dric;

import java.io.FileReader;
import java.io.Reader;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Vehicle5GSensorMain {
	public static void main(String... args) throws Exception {
		try ( Reader reader = new FileReader("mar_vehicle_framemeta_20200203-170112.json") ) {
			JsonArray vehicles = new JsonParser().parse(reader).getAsJsonArray();
			long cnt = FStream.from(vehicles.iterator())
					.map(JsonElement::getAsJsonObject)
					.map(Vehicle5GSensor::parseJson)
					.count();
			System.out.println(cnt);
		}
		try ( Reader reader = new FileReader("mar_person_framemeta_20200203-170111.json") ) {
			JsonArray vehicles = new JsonParser().parse(reader).getAsJsonArray();
			long cnt = FStream.from(vehicles.iterator())
					.map(JsonElement::getAsJsonObject)
					.map(Person5GSensor::parseJson)
					.count();
			System.out.println(cnt);
		}
	}
}
