import { Kafka } from 'kafkajs';
import { S3Client, GetObjectCommand } from '@aws-sdk/client-s3';

const s3= new S3Client({});
const kafka = new Kafka({
  clientId: 'sample-app',
  brokers: ['localhost:9092'],
});

export const handler = async (event, context) => {
   
    const bucket = event.Records[0].s3.bucket.name;
    const key = decodeURIComponent(event.Records[0].s3.object.key.replace(/\+/g, ' '));
    const params = {
        Bucket: bucket,
        Key: key,
    };

    const producer = async (str) => {
        const producer = kafka.producer();
        console.log(producer)
        await producer.connect();
        await producer.send({
          topic: 'test-topic',
          messages: [{ value: str }],
        });
        await producer.disconnect();
    };
      
    try {
        console.log("発火")
        // const result = await s3.getObject(params).promise();
        // console.log('CONTENT TYPE:', ContentType);
        // console.log({result})
        // console.log(result.Body.toString())
        // producer(result.Body.toString());

    
        const getCommand = new GetObjectCommand(params);
        const res = await s3.send(getCommand);  
        console.log("発火")
        // Stream.Readable型に変換 
        const body = res.Body
          
        // Stream.Readableを文字列に変換する
        let fileContents = '';
        for await (const chunk of body) {
          fileContents += chunk;
        }
        console.log(fileContents)
        await producer(result.Body.toString());
    } catch (err) {
        console.log(err.message);
    }
};