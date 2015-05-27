/*
 * Licensed to ElasticSearch under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.river.activemq;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.*;

import javax.jms.*;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * @author Dominik Dorn // http://dominikdorn.com
 * @author Alexander Pakulov <a.pakulov@gmail.com>
 */
@ElasticsearchIntegrationTest.ClusterScope(
        scope = ElasticsearchIntegrationTest.Scope.SUITE,
        numDataNodes = 1,
        numClientNodes = 0,
        transportClientRatio = 0.0)
public class ActiveMQRiverTest extends ElasticsearchIntegrationTest {

    final String message = "{ \"index\" : { \"_index\" : \"test\", \"_type\" : \"type1\", \"_id\" : \"1\" }\n" +
            "{ \"type1\" : { \"field1\" : \"value1\" } }\n" +
            "{ \"delete\" : { \"_index\" : \"test\", \"_type\" : \"type1\", \"_id\" : \"2\" } }\n" +
            "{ \"create\" : { \"_index\" : \"test\", \"_type\" : \"type1\", \"_id\" : \"1\" }\n" +
            "{ \"type1\" : { \"field1\" : \"value1\" } }";

    final String message2 = "{ \"index\" : { \"_index\" : \"test2\", \"_type\" : \"type2\", \"_id\" : \"1\" }\n" +
            "{ \"type2\" : { \"field2\" : \"value2\" } }\n" +
            "{ \"delete\" : { \"_index\" : \"test2\", \"_type\" : \"type2\", \"_id\" : \"2\" } }\n" +
            "{ \"create\" : { \"_index\" : \"test2\", \"_type\" : \"type2\", \"_id\" : \"1\" }\n" +
            "{ \"type2\" : { \"field2\" : \"value2\" } }";

    private static BrokerService broker;

    @BeforeClass
    public static void startActiveMQBroker() throws Exception {
        broker = new BrokerService();
        broker.setUseJmx(false);
        broker.addConnector("tcp://localhost:61616");
        broker.setUseShutdownHook(true);
        broker.start();
    }

    @AfterClass
    public static void stopActiveMQBroker() throws Exception {
//        for (TransportConnector connector : broker.getTransportConnectors()) {
//            try {
//                connector.stop();
//                broker.removeConnector(connector);
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        }

        broker.getBroker().stop();
    }

    @Override
    protected int numberOfReplicas() {
        return 0;
    }

    @Override
    protected int numberOfShards() {
        return 1;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.settingsBuilder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("plugins." + PluginsService.LOAD_PLUGIN_FROM_CLASSPATH, true)
                .build();
    }

    @Test
    public void testSimpleScenario() throws Exception {
        internalCluster().client().prepareIndex("_river", "test1", "_meta")
                .setSource(jsonBuilder().startObject()
                        .field("type", "activemq")
                        .endObject())
                .execute().actionGet();

        assertFalse(internalCluster().client().admin().indices().prepareExists("test").execute().actionGet().isExists());

        // connect to the ActiveMQ Broker and publish a message into the default queue
        postMessageToQueue(ActiveMQRiver.defaultActiveMQSourceName, message);

        Thread.sleep(3000l);

        GetResponse resp = internalCluster().client().prepareGet("test", "type1", "1").execute().actionGet();
        assertEquals("{ \"type1\" : { \"field1\" : \"value1\" } }", resp.getSourceAsString());
    }

    @Test
    public void testRiverWithNonDefaultName() throws Exception {

        String riverCustomSourceName = "nonDefaultNameQueue";

        internalCluster().client().prepareIndex("_river", "test2", "_meta")
                .setSource(jsonBuilder()
                        .startObject()
                            .field("type", "activemq")
                            .startObject("activemq")
                                    .field("sourceName", riverCustomSourceName)
                                    .field("connectionRestart", 1)
                            .endObject()
                        .endObject())
                .execute().actionGet();

        // assure that the index is not yet there
        assertFalse(internalCluster().client().admin().indices().prepareExists("test2").execute().actionGet().isExists());

        postMessageToQueue(riverCustomSourceName, message2);
        Thread.sleep(3000l);

        GetResponse resp = internalCluster().client().prepareGet("test2", "type2", "1").execute().actionGet();
        assertEquals("{ \"type2\" : { \"field2\" : \"value2\" } }", resp.getSourceAsString());
    }

    private void postMessageToQueue(final String sourceName, final String msgText) {
        // connect to the ActiveMQ Broker and publish a message into the default queue
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory();
        try {
            Connection conn = factory.createConnection();
            Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue(sourceName);
            MessageProducer producer = session.createProducer(queue);
            producer.send(session.createTextMessage(msgText));

            session.close();
            conn.close();
        } catch (JMSException e) {
            Assert.fail("JMS Exception");
        }
    }
}
