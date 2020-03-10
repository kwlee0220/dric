package dric;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.google.common.collect.Lists;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import marmot.Record;
import marmot.command.MarmotConnector;
import marmot.protobuf.PBRecordProtos;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Vehicle5GSensorMain {
	private static final String DATASET = "dric/vehicle";
	
	public static void main(String... args) throws Exception {
		JsonParser parser = new JsonParser();
		KafkaProducer<String,byte[]> uploader = createUploader(MarmotConnector.getKafkaBroker().get());
		
		long count = 0;
		KafkaConsumer<String,String> consumer = create5GSensorConsumer("129.254.82.224:9092",
																	"marmot_vehicle","T_VEHICLES");
		while ( true ) {
			ConsumerRecords<String, String> fetcheds;
			try {
				fetcheds = consumer.poll(1000);
				if ( fetcheds.isEmpty() ) {
					break;
				}
				
				for ( ConsumerRecord<String,String> crec: fetcheds ) {
					ProducerRecord<String,byte[]> prodRec = toProducerRecord(DATASET, parser, crec);
					uploader.send(prodRec);
					
					++count;
				}
			}
			catch ( WakeupException expected ) { }
		}
		
		System.out.println("count: " + count);
	}
	
	private static ProducerRecord<String,byte[]> toProducerRecord(String dsId, JsonParser parser,
																ConsumerRecord<String,String> crec) {
		JsonObject jobj = parser.parse(crec.value()).getAsJsonObject();
		Record sensorRec = Vehicle5GSensor.parseJson(jobj).toRecord();
		byte[] encoded = PBRecordProtos.toProto(sensorRec).toByteArray();
		return new ProducerRecord<>("marmot_kafka_import", dsId, encoded);
	}
	
	private static KafkaConsumer<String,String>
	create5GSensorConsumer(String broker, String grpId, String topic) {
		Properties configs = new Properties();
		configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
		configs.put(ConsumerConfig.GROUP_ID_CONFIG, grpId);
		configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
									StringDeserializer.class.getName());
		
		KafkaConsumer<String,String> consumer = new KafkaConsumer<>(configs);
		consumer.subscribe(Lists.newArrayList(topic));
		
		return consumer;
	}
	
	private static KafkaProducer<String,byte[]> createUploader(String broker) {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
		
		return new KafkaProducer<>(props);
	}
}
