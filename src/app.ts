import { Admin, ITopicConfig, Kafka, Partitioners } from 'kafkajs';
import ProducerFactory from './kafka/producer';
import { ConsumerFactory, corezoidConfig } from './kafka/consumer';

interface kafkaConfig {
  clientId: string;
  brokers: string[];
  ssl: boolean
}

interface consumerObj {
  groupId: string;
}

interface topicObj {
  validateOnly: boolean | undefined;
  waitForLeaders: boolean | undefined;
  timeout: number | undefined;
  topics: ITopicConfig[];
}

export class KafkaFactory {
  kafka: Kafka;
  admin: Admin;

  constructor(kafkaConfig: kafkaConfig) {
    this.kafka = this.createKafka(kafkaConfig);
    this.admin = this.kafka.admin();
  }

  private shutdown() {
    this.admin.disconnect();
  }

  async send(message: object | [], topic: string) {

    const listTopics = await this.getActualTopics();
    if(!listTopics.find(group => group == topic)) {
      return {result: 'Topic does not exist'};
    } else {
      const producer = new ProducerFactory(
        this.kafka.producer({
          createPartitioner: Partitioners.LegacyPartitioner,
        })
      );
  
      await producer.start();
      const result = await producer.send(message, topic);
      return {result};
    }
  }

  async createTopics(topicObj: topicObj) {
    try {
      const result = await this.admin.createTopics(topicObj);
      this.shutdown();
      return result;
    } catch (e) {
      return e;
    }
  }

  async getActualTopics() {
    const listTopics = await this.admin.listTopics();
    this.shutdown();
    return listTopics;
  }

  async getListGroups() {
    try {
      const listGroups = await this.admin.listGroups();
      this.shutdown();
      return listGroups;
    } catch(e) {
      return e
    }
  }

  async read(
    topic: string,
    consumerObj: consumerObj,
    corezoidConfig: corezoidConfig
  ) {
      const consumer = new ConsumerFactory(this.kafka.consumer(consumerObj));
      await consumer.startBatchConsumer(topic, corezoidConfig);
  }

  private createKafka(kafkaConfig: kafkaConfig): Kafka {
    const kafka = new Kafka(kafkaConfig);
    return kafka;
  }
}
