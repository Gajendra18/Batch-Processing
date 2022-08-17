package com.project.customs;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.project.utility.CsvParser;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.batch.item.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import javax.annotation.PostConstruct;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class CustomMultiFileReader implements ItemStreamReader<Document> {

	private int count;

	private List<Document> users;

	private static final String CURRENT_INDEX = "current.index";
	
	@Value("${application.bucket.name}")
	private String bucketName;

	@Value("${cloud.aws.region.static}")
	private String region;

	@Autowired
	private AmazonS3 s3Client;

	@Autowired
	private CsvParser csvParser;

	private final List<String> payload;

	public CustomMultiFileReader(List<String> payload) {
		this.payload = payload;
	}

	@Override
	public void open(ExecutionContext executionContext) throws ItemStreamException {
		if (executionContext.containsKey(CURRENT_INDEX)) {
			this.count = Integer.parseInt(Long.toString(executionContext.getLong(CURRENT_INDEX)));
		} else {
			this.count = 0;
		}

	}

	@Override
	public void update(ExecutionContext executionContext) throws ItemStreamException {
		executionContext.put(CURRENT_INDEX, (long) this.count);
	}

	@Override
	public Document read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
		Document user = null;
		if (this.count < this.users.size()) {
			user = this.users.get(this.count);
			this.count++;
		} else {
			this.count = 0;
		}
		return user;
	}

	@Override
	public void close() throws ItemStreamException {
		// Default Implementation
	}


	@PostConstruct
	public void mapInput() throws Exception {
		this.users = new ArrayList<>();
		this.count = 0;

		try {
			for (String file : this.payload) {
				S3Object object = s3Client.getObject(bucketName, file);
				try (S3ObjectInputStream inputStream = object.getObjectContent();
						InputStreamReader isr = new InputStreamReader(inputStream);
						BufferedReader br = new BufferedReader(isr)) {
					String line = null;
					String[] headers = br.readLine().split(",");
					while ((line = br.readLine()) != null) {
						String[] array = line.split(",");
						Document doc = new Document();

						int i = 0;
						for (String fieldName : headers) {
							doc.put(fieldName, csvParser.stringToDataType(array[i]));
							i++;
						}
						this.users.add(doc);
					}
				}finally {
					object.close();
				}
			}
		} catch (Exception e) {
			log.info(new StringBuffer(e.getMessage()).append(e.getStackTrace()).toString());
		}
		
	}

}
