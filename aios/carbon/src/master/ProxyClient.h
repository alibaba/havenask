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
// carbon-proxy client
#ifndef CARBON_MASTER_PROXYCLIENT_H
#define CARBON_MASTER_PROXYCLIENT_H

#include "aios/network/anet/anet.h"
#include "common/common.h"
#include "master/Flag.h"
#include "carbon/Log.h"
#include "carbon/GroupPlan.h"
#include "carbon/Status.h"
#include "carbon/proto/Status.pb.h"
#include "master/HttpClient.h"
#include <google/protobuf/message.h>

BEGIN_CARBON_NAMESPACE(master);

#define CONTENT_TYPE_JSON "application/json" 
#define CONTENT_TYPE_PB "application/x-protobuf"

class HttpClient;
CARBON_TYPEDEF_PTR(HttpClient);


struct ProxyWorkerNodePreference: public autil::legacy::Jsonizable {
    ProxyWorkerNodePreference():scope(HIPPO_PREFERENCE_SCOPE_APP), type(HIPPO_PREFERENCE_TYPE_PROHIBIT), ttl(HIPPO_PREFERENCE_DEFAULT_TTL) {}
    ProxyWorkerNodePreference(
        const std::string &scope, 
        const std::string &type, 
        uint64_t ttlSec): scope(scope), type(type), ttl(ttlSec) {}

    JSONIZE() {
        JSON_FIELD(scope);
        JSON_FIELD(type);
        JSON_FIELD(ttl);
    }
    std::string scope;
    std::string type;
    uint64_t ttl;
};

struct ReclaimWorkerNode : public autil::legacy::Jsonizable {
    ReclaimWorkerNode() {}
    ReclaimWorkerNode(
        const std::string& workerId, 
        const SlotId& slot): workerNodeId(workerId), slotId(slot){}

    JSONIZE() {
        JSON_FIELD(workerNodeId);
        JSON_FIELD(slotId);
    }

    bool operator==(const ReclaimWorkerNode& rhs) const {
        return rhs.workerNodeId == workerNodeId && rhs.slotId == slotId;
    }

    std::string workerNodeId;
    SlotId slotId;
};

class ProxyClient
{
private:
    struct BaseResponse : public autil::legacy::Jsonizable{
        BaseResponse() : code(-1), subCode(0), msg("no response") {}
        JSONIZE() {
            JSON_FIELD(code);
            JSON_FIELD(subCode);
            JSON_FIELD(msg);
        }
        bool isSuccess() const { return code == 0 && subCode == 0; }

        int code;
        int subCode;
        std::string msg;
    };
    typedef BaseResponse CommonResponse;

    struct StatusResponse : public BaseResponse {
        JSONIZE() {
            BaseResponse::Jsonize(json);
            JSON_FIELD(data);
        }
        std::vector<GroupStatus> data;
    };

    struct ReclaimWorkerNodesBody : public autil::legacy::Jsonizable {
        ReclaimWorkerNodesBody(
            const std::vector<ReclaimWorkerNode> &workerNodes,
            ProxyWorkerNodePreference *pref): workerNodes(workerNodes), preference(pref) {}
        JSONIZE() {
            JSON_FIELD(workerNodes);
            JSON_FIELD(preference);
        }
        std::vector<ReclaimWorkerNode> workerNodes;
        ProxyWorkerNodePreference *preference;
    };
    
public:
    // host: ip:port
    ProxyClient(const std::string& host, const std::string& app, bool single, bool useJsonEncode);
    virtual ~ProxyClient() {}

    MOCKABLE bool addGroup(const GroupPlan &plan, ErrorInfo *errorInfo);
    MOCKABLE bool updateGroup(const GroupPlan &plan, ErrorInfo *errorInfo);
    MOCKABLE bool delGroup(const groupid_t &groupId, ErrorInfo *errorInfo);
    MOCKABLE bool setGroups(const GroupPlanMap& groupPlanMap, ErrorInfo *errorInfo);
    MOCKABLE bool getGroupStatusList(const std::vector<groupid_t>& ids, std::vector<GroupStatus>& statusList, ErrorInfo *errorInfo);
    MOCKABLE bool reclaimWorkerNodes(const std::vector<ReclaimWorkerNode> &workerNodeNames, 
        ProxyWorkerNodePreference *pref, ErrorInfo* errorInfo);

private:
    std::string formatSchOptions(bool);
    template <typename T>
    bool doRequest(anet::HTTPPacket::HTTPMethod method, const std::string& api, const T& req, BaseResponse* result);
   
    bool parseResult(const std::string& api, QueryResult &httpResult, proto::Response* result);
    bool parseResult(const std::string& api, QueryResult &httpResult, BaseResponse* result);

    template <typename T>
    bool doQuery(anet::HTTPPacket::HTTPMethod method, const std::string& api, 
                  const T& req, 
                  QueryResult &httpResult, 
                  const std::string& accept = CONTENT_TYPE_JSON, bool verbose = true);
 private:
    HttpClientPtr _http;
    std::string _host;
    std::string _app;
    std::string _accept;
    bool _single;
    bool _useJsonEncode;
    CARBON_LOG_DECLARE();
};

CARBON_TYPEDEF_PTR(ProxyClient);

END_CARBON_NAMESPACE(master);

#endif
