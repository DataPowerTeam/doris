// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.


package org.apache.doris.common.util;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.UserException;
import org.apache.doris.proto.InternalService;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatusCode;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class PulsarUtil {
    private static final Logger LOG = LogManager.getLogger(PulsarUtil.class);

    private static final ProxyAPI PROXY_API = new ProxyAPI();

    public static List<String> getAllPulsarPartitions(String serviceUrl, String topic, String subscription,
                                                       ImmutableMap<String, String> properties) throws UserException {
        return PROXY_API.getAllPulsarPartitions(serviceUrl, topic, subscription, properties);
    }

    public static Map<String, Long> getBacklogNums(String serviceUrl, String topic, String subscription,
                                                    ImmutableMap<String, String> properties,
                                                    List<String> partitions) throws UserException {
        return PROXY_API.getBacklogNums(serviceUrl, topic, subscription, properties, partitions);
    }

    public static List<InternalService.PPulsarBacklogProxyResult> getBatchBacklogNums(
            List<InternalService.PPulsarBacklogProxyRequest> requests)
            throws UserException {
        return PROXY_API.getBatchBacklogNums(requests);
    }

    public static InternalService.PPulsarLoadInfo genPPulsarLoadInfo(String serviceUrl,
                                                                     String topic, String subscription,
                                                   ImmutableMap<String, String> properties) {
        InternalService.PPulsarLoadInfo pulsarLoadInfo = new InternalService.PPulsarLoadInfo();
        pulsarLoadInfo.setServiceUrl(serviceUrl);
        pulsarLoadInfo.setTopic(topic);
        pulsarLoadInfo.setSubscription(subscription);
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            InternalService.PStringPair pair = new InternalService.PStringPair();
            pair.setKey(entry.getKey());
            pair.setVal(entry.getValue());
            if (pulsarLoadInfo.getProperties() == null) {
                pulsarLoadInfo.getProperties() = Lists.newArrayList();
            }
            pulsarLoadInfo.getProperties().add(pair);
        }
        return pulsarLoadInfo;
    }

    static class ProxyAPI {
        public List<String> getAllPulsarPartitions(String serviceUrl, String topic, String subscription,
                                                   ImmutableMap<String, String> convertedCustomProperties)
                throws UserException {
            // create request
            InternalService.PPulsarMetaProxyRequest metaRequest = InternalService.PPulsarMetaProxyRequest().newBuilder()
                    .setPulsarInfo(genPPulsarLoadInfo(serviceUrl, topic, subscription, convertedCustomProperties));
            InternalService.PPulsarProxyRequest request = InternalService.PPulsarProxyRequest().newBuilder()
                    .setPulsarMetaRequest(metaRequest);

            InternalService.PPulsarProxyResult result = sendProxyRequest(request);
            return result.getPulsarMetaResult().getPartitions();
        }

        public Map<String, Long> getBacklogNums(String serviceUrl, String topic, String subscription,
                                                ImmutableMap<String, String> properties, List<String> partitions)
                throws UserException {
            // create request
            InternalService.PPulsarBacklogProxyRequest backlogRequest = InternalService.PPulsarBacklogProxyRequest()
                    .newBuilder().setPartitions(partitions)
                    .setPulsarInfo(genPPulsarLoadInfo(serviceUrl, topic, subscription, properties));
            InternalService.PPulsarProxyRequest request = InternalService.PPulsarProxyRequest().newBuilder()
                    .setPulsarBacklogRequest(backlogRequest);

            // send request
            InternalService.PPulsarProxyResult result = sendProxyRequest(request);

            // assembly result
            Map<String, Long> partitionBacklogs = Maps.newHashMapWithExpectedSize(partitions.size());
            List<Long> backlogs = result.getPulsarBacklogResult().getBacklogNums;
            for (int i = 0; i < result.getPulsarBacklogResult().getPartitions().size(); i++) {
                partitionBacklogs.put(result.getPulsarBacklogResult().getPartitions().get(i), backlogs.get(i));
            }
            return partitionBacklogs;
        }

        public List<InternalService.PPulsarBacklogProxyResult> getBatchBacklogNums(
                List<InternalService.PPulsarBacklogProxyRequest> requests)
                throws UserException {
            // create request
            InternalService.PPulsarBacklogBatchProxyRequest pPulsarBacklogBatchProxyRequest =
                    InternalService.PPulsarBacklogBatchProxyRequest().newBuilder().setRequests(requests);
            InternalService.PPulsarProxyRequest pProxyRequest = InternalService.PPulsarProxyRequest().newBuilder()
                    .setPulsarBacklogBatchRequest(pPulsarBacklogBatchProxyRequest);

            // send request
            InternalService.PPulsarProxyResult result = sendProxyRequest(pProxyRequest);

            return result.getPulsarBacklogBatchResult().getResults();
        }

        private InternalService.PPulsarProxyResult sendProxyRequest(
                InternalService.PPulsarProxyRequest request) throws UserException {
            TNetworkAddress address = new TNetworkAddress();
            try {
                // TODO: need to refactor after be split into cn + dn
                List<Long> nodeIds = new ArrayList<>();
                nodeIds = Env.getCurrentSystemInfo().getAllBackendIds(true);
                if (nodeIds.isEmpty()) {
                    throw new LoadException("Failed to send proxy request. No alive backends");
                }

                Collections.shuffle(nodeIds);

                Backend be = Env.getCurrentSystemInfo().getBackend(nodeIds.get(0));
                address = new TNetworkAddress(be.getHost(), be.getBrpcPort());

                // get info
                request.setTimeout(10);
                Future<InternalService.PPulsarProxyResult> future =
                        BackendServiceProxy.getInstance().getPulsarInfo(address, request);
                InternalService.PPulsarProxyResult result = future.get(10, TimeUnit.SECONDS);
                TStatusCode code = TStatusCode.findByValue(result.getStatus().getStatusCode());
                if (code != TStatusCode.OK) {
                    LOG.warn("failed to send proxy request to "
                            + address + " err " + result.getStatus().getErrorMsgsList());
                    throw new UserException(
                            "failed to send proxy request to " + address
                                + " err " + result.getStatus().getErrorMsgsList());
                } else {
                    return result;
                }
            } catch (InterruptedException ie) {
                LOG.warn("got interrupted exception when sending proxy request to " + address);
                Thread.currentThread().interrupt();
                throw new LoadException("got interrupted exception when sending proxy request to " + address);
            } catch (Exception e) {
                LOG.warn("failed to send proxy request to " + address + " err " + e.getMessage());
                throw new LoadException("failed to send proxy request to " + address + " err " + e.getMessage());
            }
        }
    }
}

