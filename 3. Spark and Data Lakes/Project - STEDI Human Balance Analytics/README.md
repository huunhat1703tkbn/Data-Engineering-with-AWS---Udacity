
# Project 3: STEDI - Human Balance Analytics

## Quick start

#### This project handles the data produced by the STEDI Step Trainer sensors and the mobile app. Our goal will be extract the data and curate them into a data lakehouse solution on AWS

## Database

####  STEDI contains three datasets, that consists of a set of files in JSON format stored in AWS S3 bucket:
- Customer: serialnumber, sharewithpublicasofdate, birthday, registrationdate, sharewithresearchasofdate, customername, email, lastupdatedate, phone, sharewithfriendsasofdate.
- Step-trainer: sensorReadingTime, serialNumber, distanceFromObject.
- Accelerometer: timeStamp, user, x, y ,z.

### Resources
- Glue Studio.
- Athena.

## Steps

### 1. Create the SQL  tables
- customer_landing.sql
- step_trainer_landing.sql
- accelerometer_trusted.sql 

### 2. Create the Trusted Zone tables
- customer_trusted.py (condition shareWithresearchAsOfDate != 0).
- accelerometer_trusted.py (accelerometer_landing JOIN customer_trusted ON user = email).
- step_trainer_trusted.py: before creating this table we must create customer_curated, then we will explain how to do it.
(step_trainer_landing JOIN customer_curated ON serialNumber = serialNumber)

### 3. Create the Curated Zone tables
- customer_curated.py (accelerometer_landing JOIN customer_trusted ON user = email).
- step_trainer_curated.py = machine_learning_curated.py (step_trainer_trusted = accelerometer_trusted ON sensorReadingTime = timeStamp).