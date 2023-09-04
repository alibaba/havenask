/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef ISEARCH_GIG_GIGAPI_H
#define ISEARCH_GIG_GIGAPI_H

namespace multi_call {

extern "C" {

typedef void (*JavaCallback)(long callbackId, char *header, int headerLen, char *response,
                             int responseLen);
__attribute__((visibility("default"))) int initGigJni(const char *alogConfStr,
                                                      const char *kmonConfStr);
__attribute__((visibility("default"))) int
createGigInstance(const char *gigConfigStr, JavaCallback callback, long &instancePtr);
__attribute__((visibility("default"))) int resetGigInstance(long instancePtr);
__attribute__((visibility("default"))) int deleteGigInstance(long instancePtr);
__attribute__((visibility("default"))) int updateDefaultFlowConfig(long instancePtr,
                                                                   const char *flowConf);
__attribute__((visibility("default"))) int
updateFlowConfig(long instancePtr, const char *flowConfigStrategy, const char *flowConf);
__attribute__((visibility("default"))) int addSubscribeService(long instancePtr,
                                                               const char *subServiceConfStr);
__attribute__((visibility("default"))) int addSubscribe(long instancePtr,
                                                        const char *gigSubConfStr);
__attribute__((visibility("default"))) int deleteSubscribe(long instancePtr,
                                                           const char *gigSubConfStr);
__attribute__((visibility("default"))) int hasBizSnapshot(long instancePtr, const char *bizName,
                                                          int &ret);
__attribute__((visibility("default"))) int call(long instancePtr, char *body, int len, char *plan,
                                                int pLen, long callbackId);

// session api
__attribute__((visibility("default"))) int sessionCreate(long instancePtr, char *sessionInfo,
                                                         long &sessionPtr);
__attribute__((visibility("default"))) int sessionDelete(long sessionPtr);
__attribute__((visibility("default"))) int sessionCall(long instancePtr, long sessionPtr,
                                                       char *body, int len, char *plan, int pLen,
                                                       long callbackId);

// provider api
__attribute__((visibility("default"))) int setTargetWeight(long instancePtr, const char *nodeId,
                                                           int weight);

// use freeString for returned bizMeta
__attribute__((visibility("default"))) int sessionBizSelect(long sessionPtr, char *plan, int len,
                                                            long callbackId);
__attribute__((visibility("default"))) int updateTopoNodes(long instancePtr, char *msg,
                                                           long msgLen);
__attribute__((visibility("default"))) int selectProvider(long instancePtr, char *msg, long msgLen,
                                                          long callbackId);
__attribute__((visibility("default"))) int
finishClientSession(long resourcePtr, char *agentInfo, int infoLen, int ec, long callbackId);

// agent api
__attribute__((visibility("default"))) int
createGigAgentInstance(char *msg, long msgLen, JavaCallback callback, long &instance);
__attribute__((visibility("default"))) int deleteGigAgentInstance(long agent);
__attribute__((visibility("default"))) int
getAgentQuerySession(long agent, char *queryInfo, int queryInfoLen, long &queryInfoPtr,
                     float &degradePercent, int &requestType);
__attribute__((visibility("default"))) int start(long agent);
__attribute__((visibility("default"))) int stop(long agent);
__attribute__((visibility("default"))) int isStopped(long agent, int &ret);
__attribute__((visibility("default"))) int startBiz(long agent, char *bizName, int nameLen);
__attribute__((visibility("default"))) int stopBiz(long agent, char *bizName, int nameLen);
__attribute__((visibility("default"))) int isStoppedBiz(long agent, char *bizName, int nameLen,
                                                        int &ret);
__attribute__((visibility("default"))) int longTimeNoQuery(long agent, long timeoutInSecond,
                                                           int &ret);
__attribute__((visibility("default"))) int
bizLongTimeNoQuery(long agent, char *bizName, int nameLen, long timeoutInSecond, int &ret);
__attribute__((visibility("default"))) int finishAgentSession(long queryInfoPtr,
                                                              float responseLatencyMs, int ec,
                                                              int targetWeight, long callbackId);

// common api
__attribute__((visibility("default"))) void freeString(char *str);
__attribute__((visibility("default"))) int finishClientSessionLatency(long resourcePtr,
                                                                      char *agentInfo, int infoLen,
                                                                      int ec, float latencyMs,
                                                                      long callbackId);
}

} // namespace multi_call

#endif // ISEARCH_GIG_GIGAPI_H
