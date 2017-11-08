/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.fhuss.kafka.streams.cep.demo;

import com.github.fhuss.kafka.streams.cep.CEPStream;
import com.github.fhuss.kafka.streams.cep.ComplexStreamsBuilder;
import com.github.fhuss.kafka.streams.cep.Sequence;
import com.github.fhuss.kafka.streams.cep.pattern.Pattern;
import com.github.fhuss.kafka.streams.cep.pattern.QueryBuilder;
import com.github.fhuss.kafka.streams.cep.processor.CEPProcessor;
import com.github.fhuss.kafka.streams.cep.state.CEPStoreBuilders;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class CEPStockKStreamsDemo {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-cep");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, StockEventSerDe.class);

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        // build query
        final Pattern<String, StockEvent> pattern = new QueryBuilder<String, StockEvent>()
                .select()
                    .where((k, v, ts, store) -> v.volume > 1000)
                    .<Long>fold("avg", (k, v, curr) -> v.price)
                    .then()
                .select()
                    .zeroOrMore()
                    .skipTillNextMatch()
                    .where((k, v, ts, state) -> v.price > (long)state.get("avg"))
                    .<Long>fold("avg", (k, v, curr) -> (curr + v.price) / 2)
                    .<Long>fold("volume", (k, v, curr) -> v.volume)
                    .then()
                .select()
                    .skipTillNextMatch()
                    .where((k, v, ts, state) -> v.volume < 0.8 * state.getOrElse("volume", 0L))
                    .within(1, TimeUnit.HOURS)
                .build();

        ComplexStreamsBuilder builder = new ComplexStreamsBuilder();

        CEPStream<String, StockEvent> stream = builder.stream("StockEvents");
        KStream<String, Sequence<String, StockEvent>> stocks = stream.query("Stocks", pattern, Serdes.String(), new StockEventSerDe());

        stocks.mapValues(seq -> {
                  JSONObject json = new JSONObject();
                  seq.asMap().forEach( (k, v) -> {
                      JSONArray events = new JSONArray();
                      json.put(k, events);
                      List<String> collect = v.stream().map(e -> e.value.name).collect(Collectors.toList());
                      Collections.reverse(collect);
                      collect.forEach(events::add);
                  });
                  return json.toJSONString();
              })
              .through("Matches", Produced.with(null, Serdes.String()))
              .print(Printed.toSysOut());

        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-cep-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
