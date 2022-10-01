
# Spring Batch Server



## Get Started

### Apache Kafka

Install Apache kafka
https://kafka.apache.org/downloads

Go to the kafka installation directory

start zookeeper
```bash
  .\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
```

start kafka server
```bash
  .\bin\windows\kafka-server-start.bat .\config\server.properties
```


### Email Alerts

- Turn on 2-Step Verification in your google account. This step is required as Google only allows generating passwords for apps on accounts that have 2-Step Verification enabled.
- Go to generate apps password (https://myaccount.google.com/apppasswords)
- Click on select app, select other (custom name) from options, enter 'BatchApp' and generate a password
- In application.properties file update your email and password
```bash
  spring.mail.username=your_email
  spring.mail.password=generated_password
```


### Amazon S2 Bucket
- Login to Amazon web services and create a bucket with name 'springbatchproject'
- In Application.properties file update access key and secret key and bucket region values

```bash
cloud.aws.credentials.access-key: your_access_key
cloud.aws.credentials.secret-key: your_secret_key
cloud.aws.region.static: bucket_region
```

### Database setup
Import sql file from Database directory

