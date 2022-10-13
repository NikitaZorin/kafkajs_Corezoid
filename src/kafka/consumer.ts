import { Consumer, ConsumerSubscribeTopics, EachBatchPayload } from 'kafkajs';



export class ConsumerFactory {
  private consumer: Consumer;

  public constructor(consumer: Consumer) {
    this.consumer = consumer;
  }

  private shutdown() {
    this.consumer.disconnect();
  }

 public async startBatchConsumer(topicName: string) {
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
              batch.messages.forEach(message => {
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
              this.shutdown();
            }
          });
    });
    } catch (error) {
      this.shutdown();
      return error;
    }
  }
}

