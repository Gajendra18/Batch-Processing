package com.project.customs;

import com.project.constants.Constants;
import com.project.entity.WorkerStatus;
import com.project.service.BatchService;
import lombok.extern.slf4j.Slf4j;
import org.jboss.logging.MDC;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.messaging.simp.SimpMessagingTemplate;

import javax.mail.internet.MimeMessage;
import java.util.Date;
import java.util.List;

@Slf4j
@Configuration
@DependsOn({"kafkaTemplate"})
@Order(Ordered.LOWEST_PRECEDENCE)
public class JobExecutionListnerClass implements JobExecutionListener {

    @Autowired
    private JavaMailSender javaMailSender;

    @Autowired
    private JobOperator jobOperator;

    @Autowired
    private BatchService batchService;

    @Autowired
    private KafkaTemplate<String, WorkerStatus> kafkaTemplate;

    @Autowired
    private SimpMessagingTemplate messagingTemplate;

    private static final String TOPIC = "WorkerStatus";

    @Value("${spring.mail.username}")
    private String senderMail;

    @Override
    public void beforeJob(JobExecution jobExecution) {
        try {
            long jid = jobExecution.getId();
            long instanceId = jobExecution.getJobId();
            MDC.put("jobExecutionId", Long.toString(jid));
            WorkerStatus statusWorker = new WorkerStatus();
            statusWorker.setJobInstanceId(instanceId);
            statusWorker.setJobExecutionId((int) jid);
            statusWorker.setStatus("started");
            kafkaTemplate.send(TOPIC, statusWorker);
            String name = jobExecution.getJobInstance().getJobName();
            Date date = jobExecution.getStartTime();
            String status = jobExecution.getStatus().toString();
            String mailRecipients = jobExecution.getJobParameters().getString("mailRecipients");
            if(!mailRecipients.isBlank()){
                beforeJobMail(instanceId, jid, name, date, status,
                        jobExecution.getJobParameters().getString("mailRecipients"));
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        try {
            long jid = jobExecution.getId();
            long instanceId = jobExecution.getJobId();

            WorkerStatus statusworker = new WorkerStatus();
            statusworker.setJobInstanceId(instanceId);
            statusworker.setJobExecutionId((int) jid);
            statusworker.setStatus("completed");
            kafkaTemplate.send(TOPIC, statusworker);
            String description = null;

            List<Throwable> list = jobExecution.getAllFailureExceptions();

            int size = jobExecution.getStepExecutions().size();

            if (size == 1) {
                description = "<h3>Job Completed : No Files Found In The Direcory<h3>";
            }
            for (Throwable exception : list) {
                if (exception.toString().contains("java.lang.NullPointerException")) {
                    description = "<h3>Job Failed : Directory Not Found<h3>";
                }
            }

            String name = jobExecution.getJobInstance().getJobName();
            Date date = jobExecution.getStartTime();
            String status = jobExecution.getStatus().toString();

            String mailRecipients = jobExecution.getJobParameters().getString("mailRecipients");
            if(!mailRecipients.isBlank()){
                afterJobMail(instanceId, jid, name, date, status,
                        jobExecution.getJobParameters().getString("mailRecipients"), description);
            }

            boolean restartStatus = Boolean
                    .parseBoolean(jobExecution.getJobParameters().getString(Constants.JOB_RESTART));
            if (jobExecution.getStatus().equals(BatchStatus.FAILED) && restartStatus
                    && jobOperator.getExecutions(jid).size() == 1) {
                batchService.restartJob(jid);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        MDC.clear();
    }

    public void beforeJobMail(long instatnceId, long jid, String name, Date date, String status,
                              String mailRecipients) {
        MimeMessage message = javaMailSender.createMimeMessage();
        String[] mail = mailRecipients.split(",");

        try {
            MimeMessageHelper helper = new MimeMessageHelper(message, true);

            for (String id : mail) {

                helper.setFrom(senderMail);
                helper.setTo(id);

                String subject = "Hey! Admin. Job Execution Has Started..";
                String jobDescription = Constants.MAIL_JOB_INSTANCE_ID + Constants.COLON + instatnceId + "\n"
                        + Constants.MAIL_JOB_EXECUTION_ID + Constants.COLON + jid + "\n" + Constants.MAIL_JOB_NAME
                        + Constants.COLON + name + "\n" + Constants.MAIL_JOB_STATUS + Constants.COLON + status + "\n"
                        + "Job " + status + Constants.MAIL_JOB_TIME + Constants.COLON + date;

                helper.setSubject(subject);
                helper.setText(jobDescription);

                javaMailSender.send(message);
            }
        } catch (Exception e) {
            log.info(e.getMessage());
        }
    }

    public void afterJobMail(long instanceId, long jid, String name, Date date, String status, String mailRecipients,
                             String description) {
        MimeMessage message = javaMailSender.createMimeMessage();
        String[] mail = mailRecipients.split(",");

        try {
            MimeMessageHelper helper = new MimeMessageHelper(message, true);

            for (String id : mail) {
                helper.setFrom(senderMail);
                helper.setTo(id);

                String subject = "Hey! Admin. Job Execution Has Ended..";

                String jobDescription = Constants.MAIL_JOB_INSTANCE_ID + Constants.COLON + instanceId + "<br>"
                        + Constants.MAIL_JOB_EXECUTION_ID + Constants.COLON + jid + "<br>" + Constants.MAIL_JOB_NAME
                        + Constants.COLON + name + "<br>" + Constants.MAIL_JOB_STATUS + Constants.COLON + status
                        + "<br>" + "Job " + status + Constants.MAIL_JOB_TIME + Constants.COLON + date;

                String jobDescription1 = Constants.MAIL_JOB_INSTANCE_ID + Constants.COLON + instanceId + "\n"
                        + Constants.MAIL_JOB_EXECUTION_ID + Constants.COLON + jid + "\n" + Constants.MAIL_JOB_NAME
                        + Constants.COLON + name + "\n" + Constants.MAIL_JOB_STATUS + Constants.COLON + status + "\n"
                        + "Job " + status + Constants.MAIL_JOB_TIME + Constants.COLON + date;

                if (description == null) {
                    helper.setText(jobDescription1);
                } else {
                    helper.setText(jobDescription + "<br>" + description);
                }

                helper.setSubject(subject);

                javaMailSender.send(message);
            }
        } catch (Exception e) {
            log.info(e.getMessage());
        }
    }

}
