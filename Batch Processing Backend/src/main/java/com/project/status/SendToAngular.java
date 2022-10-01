package com.project.status;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

import com.project.entity.WorkerStatus;

import lombok.extern.slf4j.Slf4j;

@Component
@Profile("!worker")
@Slf4j
public class SendToAngular {
	
	@Autowired
	private SimpMessagingTemplate messagingTemplate;

	
	@KafkaListener(topics = "WorkerStatus", groupId = "Status", containerFactory = "kafkaListenerContainerFactory")
	void sendStatus(WorkerStatus input) {
		messagingTemplate.convertAndSend("/topic/public",input);
	}





}
