package dric;

import org.apache.log4j.PropertyConfigurator;

import marmot.command.MarmotClientCommands;
import marmot.optor.CreateDataSetOptions;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Create5GSensorDataSet {
	private static final String PERSON = "dric/person";
	private static final String VEHICLE = "dric/vehicle";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		marmot.createDataSet(PERSON, Person5GSensor.getRecordSchema(), CreateDataSetOptions.FORCE);
		marmot.createDataSet(VEHICLE, Vehicle5GSensor.getRecordSchema(), CreateDataSetOptions.FORCE);
	}
}
