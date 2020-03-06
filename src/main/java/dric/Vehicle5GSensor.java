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
public class Vehicle5GSensor {
	private static final DateTimeFormatter FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss.SSS")
																	.withLocale(Locale.ENGLISH);
	private static final RecordSchema SCHEMA = RecordSchema.builder()
															.addColumn("score", DataType.FLOAT)
															.addColumn("uid", DataType.STRING)
															.addColumn("ts", DataType.DATETIME)
															.addColumn("speed", DataType.FLOAT)
															.addColumn("bbox", DataType.ENVELOPE)
															.addColumn("type", DataType.STRING)
															.addColumn("type_score", DataType.FLOAT)
															.addColumn("oid", DataType.LONG)
															.addColumn("direction", DataType.STRING)
															.build();
	
	private final float m_score;
	private final String m_uid;
	private final long m_timestamp;
	private final float m_speed;
	private final Envelope m_bbox;
	private final String m_type;
	private final float m_typeScore;
	private final long m_oid;
	private final String m_direction;

	Vehicle5GSensor(float score, String uid, long ts, float speed, Envelope bbox, String type,
					float typeScore, long oid, String direction) {
		m_score = score;
		m_uid = uid;
		m_timestamp = ts;
		m_speed = speed;
		m_bbox = bbox;
		m_type = type;
		m_typeScore = typeScore;
		m_oid = oid;
		m_direction = direction;
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
		output.set(3, m_speed);
		output.set(4, m_bbox);
		output.set(5, m_type);
		output.set(6, m_typeScore);
		output.set(7, m_oid);
		output.set(8, m_direction);
	}
	
	public static Vehicle5GSensor read(Record input) {
		return new Vehicle5GSensor(input.getFloat(0), input.getString(1), input.getLong(2),
									input.getFloat(3), (Envelope)input.get(4), input.getString(5),
									input.getFloat(6), input.getLong(7), input.getString(8));
	}
	
	public static Vehicle5GSensor parseJson(JsonObject json) {
		float score = json.get("score").getAsFloat();
		String uid = json.get("uid").getAsString();
		String tmStr = json.get("tm").getAsString();
		long ts = LocalDateTimes.toUtcMillis(LocalDateTime.parse(tmStr, FORMAT));
		float speed = json.get("speed").getAsFloat();
		
		JsonObject jbbox = json.getAsJsonObject("box");
		int x = jbbox.get("x").getAsInt();
		int y = jbbox.get("y").getAsInt();
		int width = jbbox.get("width").getAsInt();
		int height = jbbox.get("height").getAsInt();
		Envelope bbox = new Envelope(x, x+width, y, y+height);
		
		String vtype = json.get("vehicle_type").getAsString();
		float vtypeScore = json.get("vehicle_score").getAsFloat();
		long oid = json.get("oid").getAsLong();
		String dir = json.get("direction").getAsString();
		
		return new Vehicle5GSensor(score, uid, ts, speed, bbox, vtype, vtypeScore, oid, dir);
	}
}
