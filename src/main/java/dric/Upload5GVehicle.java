package dric;

import java.io.FileReader;
import java.io.Reader;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.PropertyConfigurator;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import marmot.Record;
import marmot.command.MarmotClientCommands;
import marmot.command.MarmotConnector;
import marmot.proto.RecordProto;
import marmot.protobuf.PBRecordProtos;
import marmot.remote.protobuf.PBMarmotClient;
import marmot.support.DefaultRecord;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Upload5GVehicle {
	private static final String DATASET = "dric/vehicle";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, MarmotConnector.getKafkaBroker().get());
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
		
		try ( Reader reader = new FileReader("mar_vehicle_framemeta_20200203-170112.json");
				KafkaProducer<String,byte[]> producer = new KafkaProducer<>(props); ) {
			Record rec = DefaultRecord.of(Person5GSensor.getRecordSchema());
			JsonArray persons = new JsonParser().parse(reader).getAsJsonArray();
			FStream.from(persons.iterator())
					.map(JsonElement::getAsJsonObject)
					.map(Vehicle5GSensor::parseJson)
					.map(Vehicle5GSensor::toRecord)
					.map(PBRecordProtos::toProto)
					.map(RecordProto::toByteArray)
					.map(b -> new ProducerRecord<>("marmot_kafka_import", DATASET, b))
					.forEach(producer::send);
		}
	}
}
