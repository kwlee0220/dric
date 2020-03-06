package dric;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

import com.google.gson.JsonObject;
import com.vividsolutions.jts.geom.Envelope;

import marmot.Record;
import marmot.RecordSchema;
import marmot.support.DefaultRecord;
import marmot.type.DataType;
import utils.LocalDateTimes;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Person5GSensor {
	private static final DateTimeFormatter FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss.SSS")
																	.withLocale(Locale.ENGLISH);
	private static final RecordSchema SCHEMA = RecordSchema.builder()
															.addColumn("score", DataType.FLOAT)
															.addColumn("uid", DataType.STRING)
															.addColumn("ts", DataType.DATETIME)
															.addColumn("oid", DataType.LONG)
															.addColumn("type", DataType.STRING)
															.addColumn("box", DataType.ENVELOPE)
															.addColumn("gender", DataType.STRING)
															.addColumn("gender_score", DataType.FLOAT)
															.addColumn("age", DataType.STRING)
															.addColumn("age_score", DataType.FLOAT)
															.addColumn("bag", DataType.STRING)
															.addColumn("bag_score", DataType.FLOAT)
															.addColumn("backpack", DataType.STRING)
															.addColumn("backpack_score", DataType.FLOAT)
															.addColumn("hat", DataType.STRING)
															.addColumn("hat_score", DataType.FLOAT)
															.build();

	private final float m_score;
	private final String m_uid;
	private final long m_timestamp;
	private final long m_oid;
	private final String m_type;
	private final Envelope m_box;
	private final String m_gender;
	private final float m_genderScore;
	private final String m_age;
	private final float m_ageScore;
	private final String m_bag;
	private final float m_bagScore;
	private final String m_backpack;
	private final float m_backpackScore;
	private final String m_hat;
	private final float m_hatScore;

	Person5GSensor(float score, String uid, long ts, long oid, String type, Envelope bbox,
					String gender, float genderScore, String age, float ageScore,
					String bag, float bagScore, String backpack, float backpackScore,
					String hat, float hatScore) {
		m_score = score;
		m_uid = uid;
		m_timestamp = ts;
		m_oid = oid;
		m_type = type;
		m_box = bbox;
		m_gender = gender;
		m_genderScore = genderScore;
		m_age = age;
		m_ageScore = ageScore;
		m_bag = bag;
		m_bagScore = bagScore;
		m_backpack = backpack;
		m_backpackScore = backpackScore;
		m_hat = hat;
		m_hatScore = hatScore;
	}
	
	public static RecordSchema getRecordSchema() {
		return SCHEMA;
	}
	
	public Record toRecord() {
		Record record = DefaultRecord.of(SCHEMA);
		write(record);
		return record;
	}
	
	public void write(Record output) {
		output.set(0, m_score);
		output.set(1, m_uid);
		output.set(2, LocalDateTimes.fromUtcMillis(m_timestamp));
		output.set(3, m_oid);
		output.set(4, m_type);
		output.set(5, m_box);
		output.set(6, m_gender);
		output.set(7, m_genderScore);
		output.set(8, m_age);
		output.set(9, m_ageScore);
		output.set(10, m_bag);
		output.set(11, m_bagScore);
		output.set(12, m_backpack);
		output.set(13, m_backpackScore);
		output.set(14, m_hat);
		output.set(15, m_hatScore);
	}
	
	public static Person5GSensor read(Record input) {
		return new Person5GSensor(input.getFloat(0), input.getString(1), input.getLong(2),
									input.getLong(3), input.getString(4), (Envelope)input.get(5),
									input.getString(6), input.getFloat(7),
									input.getString(8), input.getFloat(9),
									input.getString(10), input.getFloat(11),
									input.getString(12), input.getFloat(13),
									input.getString(14), input.getFloat(15));
	}
	
	public static Person5GSensor parseJson(JsonObject json) {
		float score = json.get("score").getAsFloat();
		String uid = json.get("uid").getAsString();
		String tmStr = json.get("tm").getAsString();
		long ts = LocalDateTimes.toUtcMillis(LocalDateTime.parse(tmStr, FORMAT));
		long oid = json.get("oid").getAsLong();
		String type = json.get("type").getAsString();
		
		JsonObject jbbox = json.getAsJsonObject("box");
		int x = jbbox.get("x").getAsInt();
		int y = jbbox.get("y").getAsInt();
		int width = jbbox.get("width").getAsInt();
		int height = jbbox.get("height").getAsInt();
		Envelope bbox = new Envelope(x, x+width, y, y+height);

		String gender = json.get("gender_type").getAsString();
		float genderScore = json.get("gender_score").getAsFloat();
		String age = json.get("age_type").getAsString();
		float ageScore = json.get("age_score").getAsFloat();
		String bag = json.get("bag_type").getAsString();
		float bagScore = json.get("bag_score").getAsFloat();
		String backpack = json.get("backpack_type").getAsString();
		float backpackScore = json.get("backpack_score").getAsFloat();
		String hat = json.get("hat_type").getAsString();
		float hatScore = json.get("hat_score").getAsFloat();
		
		return new Person5GSensor(score, uid, ts, oid, type, bbox, gender, genderScore,
								age, ageScore, bag, bagScore, backpack, backpackScore,
								hat, hatScore);
	}
}
