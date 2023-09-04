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
#ifndef ISEARCH_MULTI_CALL_GIGINSTANCE_H
#define ISEARCH_MULTI_CALL_GIGINSTANCE_H

#include "aios/network/gig/multi_call/agent/GigAgent.h"
#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/interface/SearchService.h"
#include "aios/network/gig/multi_call/java/GigApi.h"
#include "autil/Lock.h"

namespace multi_call {

struct GigInstance;
struct GigAgentInstance;
#define GIG_INSTANCE_CHECK()                                                                       \
    GigInstance *gigInstance = (GigInstance *)instancePtr;                                         \
    if (!gigInstance) {                                                                            \
        return GIG_ERROR_INVALID_PARAM;                                                            \
    }                                                                                              \
    SearchServicePtr searchService = gigInstance->getSearchServicePtr();                           \
    if (!searchService) {                                                                          \
        return GIG_ERROR_INVALID_PARAM;                                                            \
    }                                                                                              \
    JavaCallback callback = gigInstance->getJavaCallback();                                        \
    if (!callback) {                                                                               \
        return GIG_ERROR_INVALID_PARAM;                                                            \
    }

#define AGENT_INSTANCE_CHECK()                                                                     \
    GigAgentInstance *gigAgentInstance = (GigAgentInstance *)agent;                                \
    GigAgent *gigAgent = gigAgentInstance->agent;                                                  \
    if (!gigAgent) {                                                                               \
        return GIG_ERROR_INVALID_PARAM;                                                            \
    }                                                                                              \
    JavaCallback callback = gigAgentInstance->callback;                                            \
    if (!callback) {                                                                               \
        return GIG_ERROR_INVALID_PARAM;                                                            \
    }

#define GIG_QUERY_SESSION_CHECK()                                                                  \
    GigSession *gigSession = reinterpret_cast<GigSession *>(sessionPtr);                           \
    if (!gigSession) {                                                                             \
        return GIG_ERROR_INVALID_PARAM;                                                            \
    }                                                                                              \
    QuerySessionPtr &session = gigSession->session;                                                \
    if (!session) {                                                                                \
        return GIG_ERROR_INVALID_PARAM;                                                            \
    }                                                                                              \
    JavaCallback sessionCallback = gigSession->callback;                                           \
    if (!sessionCallback) {                                                                        \
        return GIG_ERROR_INVALID_PARAM;                                                            \
    }

struct GigInstance {
public:
    GigInstance(SearchServicePtr &searchService, JavaCallback &callback)
        : _searchService(searchService)
        , _callback(callback) {
    }
    ~GigInstance() {
    }

public:
    static bool isInited();
    static bool initAlog(const std::string &gigAlogConf);
    static bool initKmon(const std::string &gigKmonConf);

public:
    void reset();
    bool isReset();
    SearchServicePtr getSearchServicePtr();
    JavaCallback getJavaCallback() const;

private:
    autil::ThreadMutex _mutex;
    SearchServicePtr _searchService;
    JavaCallback _callback;

private:
    static bool alogInited;
    static bool kmonInited;

private:
    AUTIL_LOG_DECLARE();
};

struct GigSession {
public:
    GigSession(QuerySessionPtr s, JavaCallback cb) : session(s), callback(cb) {
    }
    QuerySessionPtr session;
    JavaCallback callback;
};

struct GigAgentInstance {
public:
    GigAgentInstance(GigAgent *a, JavaCallback cb) : agent(a), callback(cb) {
    }
    ~GigAgentInstance() {
        DELETE_AND_SET_NULL(agent);
    }
    GigAgent *agent;
    JavaCallback callback;
};

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_GIGINSTANCE_H
