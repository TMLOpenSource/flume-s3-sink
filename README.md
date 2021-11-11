# S3 Sink for Apache flume

## Intro
This sink continuously reads from flume source and creates a file locally on disk. It uses AWS Java SDK to upload files to S3 (this is a one time upload of the file, instead of append and renaming) after a given time interval. 

After upload it deletes the file from the disk, making the disk available for new file creations.
It can also compress the file before uploading it to S3. As of now, it supports GZ compression. 

It supports custom, header based file name and path name creation. e.g. we can create folders on S3 with configuration as s3://bucket/date=<dd-mm-yy>/key={any parameter from message}. 
Key can be extracted using interceptors.

This sink does not do any S3 network operation while appending to the file hence this sink is much faster than HDFS Sink (https://flume.apache.org/FlumeUserGuide.html#hdfs-sink). It also has a support for max open files by the sink (this helps is resolving any ulimit related issues that may occur). It allows you to configure a thread pool to do upload. This helps in faster and parallel upload to S3.

## How to install


1. Clone the project.
2. Run `mvn install`
3. Copy `flume-s3-sink-x.x.x-jar-with-dependencies.jar` to flume's lib folder
4. Add sink configuration to upload to S3, similar to below

```
a1.sinks.s1.type = com.tml.sinks.S3Sink
a1.sinks.s1.s3.bucket = {s3-bucket-name}
a1.sinks.s1.s3.accessKey = {aws-access-key}
a1.sinks.s1.s3.secretKey = {aws-secret-key}
a1.sinks.s1.s3.awsRegion = {aws-region}
a1.sinks.s1.s3.path = {path in s3 bucket}
a1.sinks.s1.s3.localDirectory = {full local base path for creation and storing files before upload to s3}
a1.sinks.s1.s3.filePrefix = {prefix for file}
a1.sinks.s1.s3.fileSuffix = {suffix for the file with file extension}
a1.sinks.s1.channel = {flume channel}
```

## Configurations
|Configuration|Data type|required|Default value|Comments|
|-----|--------|---------|-------|------|
|type|string|Y|com.tml.sinks.S3Sink|This is fixed value that has to be provided|
|bucket|string|Y| |S3 bucket name|
|accessKey|string|Y| |AWS access key|
|secretKey|string|Y| |AWS secret key|
|awsRegion|string|Y| |AWS region|
|path|string|Y| |This is relative folder path under s3 bucket. It can contain place holder for dynamically replacing values to determining absolute path. It should not start with `/`. e.g. %{key1}/fixed-name1/fixed-name2/%{key2}|
|localDirectory|string|Y| |Absolute path on the local file system to store the files before uploading to S3|
|filePrefix|string|Y| |prefix to use for file name. This will be visible in uploaded file name. It can contain place holder e.g. %{key}_name|
|fileSuffix|string|Y| |suffix to use for file name. This will be visible in uploaded file name. It can contain place holder e.g. %{key}.json|
|channel|string|Y| |configured flume channel name|
|maxOpenFiles|int|N|5000|Max open files, at a time, by the sink on local system|
|batchSize|int|N|100|Batch size to fetch from channel for processing|
|codeC|string|N|NULL|File compression done on file before uploading to S3. This can help to reduce S3 storage cost|
|rollInterval|int|N|30|Regular time interval to upload files to S3 in seconds.|
|rollTimerPoolSize|int|N|5|Max number files that can be pushed to S3 at a time.|
|fileBufferSize|int|N|8192|Max buffer size in bytes used for `FileOutputStream`.|

# Limitations
1. Currently, it only supports Gzip compression. But code is extensible to add more codec
2. It reads the messages as text. Other formats (e.g. Avro) are Currently not supported