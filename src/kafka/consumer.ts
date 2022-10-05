import { Consumer, ConsumerSubscribeTopics, EachBatchPayload, EachMessagePayload } from 'kafkajs';

interface corezoidConfig {
  url: string;
  method: string;
  data: {messages: object[]};
}

export class ConsumerFactory {
  private consumer: Consumer;

  public constructor(consumer: Consumer) {
    this.consumer = consumer;
  }

  private shutdown() {
    process.exit(1);
    // console.log(this.consumer)
    // this.consumer.disconnect();
  }

  // public sendToCorezoid(corezoidConfig: corezoidConfig) {
  //   return new Promise(resolve => {
  //     axios(corezoidConfig)
  //         .then(function(response) {
  //             resolve(response.data);
  //         })
  //         .catch(function(error) {
  //             resolve(error);
  //         });
  // });
  // }

 public async startBatchConsumer(topicName: string) {
    const topic: ConsumerSubscribeTopics = {
      topics: [topicName],
      fromBeginning: true
    }


    try {
      await this.consumer.connect();
      await this.consumer.subscribe(topic);

      return new Promise(async resolve => {
        this.consumer.run({
            eachBatchAutoResolve: true,
            eachBatch: async (eachBatchPayload: EachBatchPayload) => {
              const { batch } = eachBatchPayload;
              const requestData: object[] = [];
              batch.messages.forEach(async message => {
                const value = message.value ? message.value.toString() : null;
                requestData.push({
                  topic: batch.topic,
                  partion: batch.partition,
                  offset: message.offset,
                  message: value,
                  timestamp: message.timestamp
                });
              });
              resolve(requestData);
              setImmediate(this.shutdown);
            }
          });
    });
    } catch (error) {
      this.shutdown();
      return error;
    }
  }
}

