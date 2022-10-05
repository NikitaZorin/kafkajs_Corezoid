import { Consumer, ConsumerSubscribeTopics, EachBatchPayload, EachMessagePayload } from 'kafkajs';
import axios from 'axios';

interface corezoidConfig {
  url: string;
  method: string;
  data: {messages: object[]};
}

class ConsumerFactory {
  private consumer: Consumer;

  public constructor(consumer: Consumer) {
    this.consumer = consumer;
  }

  async shutdown() {
    this.consumer.disconnect();
  }

  public sendToCorezoid(corezoidConfig: corezoidConfig) {
    return new Promise(resolve => {
      axios(corezoidConfig)
          .then(function(response) {
              resolve(response.data);
          })
          .catch(function(error) {
              resolve(error);
          });
  });
  }

  public async startBatchConsumer(topicName: string, corezoidConfig: corezoidConfig) {
    const topic: ConsumerSubscribeTopics = {
      topics: [topicName],
      fromBeginning: true
    }


    try {
      await this.consumer.connect();
      await this.consumer.subscribe(topic);

      return new Promise(resolve => {
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
  
            corezoidConfig.data.messages = requestData;
            // await this.sendToCorezoid(corezoidConfig);
            this.shutdown();
            resolve(requestData);
          }
        })
    });
    } catch (error) {
      this.shutdown();
      return error;
    }
  }
}

export { ConsumerFactory, corezoidConfig };
