package com.project.batchconfiguration;

import com.project.exception.BatchException;
import com.project.service.BatchService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;

@Configuration
@Profile("!worker")
public class StartupEvent {

    @Autowired
    private BatchService batchService;


    @EventListener(ContextRefreshedEvent.class)
    public void cleanJobs() throws BatchException {
        batchService.startupClean();
    }
}
