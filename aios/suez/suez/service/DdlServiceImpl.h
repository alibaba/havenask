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
#pragma once

#include "aios/network/gig/multi_call/interface/SearchService.h"
#include "suez/sdk/SearchManager.h"
#include "suez/service/Service.pb.h"

namespace kmonitor {
class MetricsReporter;
}

namespace suez {

class LeaderRouter;

class DdlServiceImpl final : public SearchManager, public DdlService {
public:
    DdlServiceImpl();
    ~DdlServiceImpl();

public:
    // SearchManager interface
    bool init(const SearchInitParam &initParam) override;
    UPDATE_RESULT update(const UpdateArgs &updateArgs,
                         UpdateIndications &updateIndications,
                         SuezErrorCollector &errorCollector) override;
    void stopService() override;
    void stopWorker() override;

    // rpc interface
    void updateSchema(google::protobuf::RpcController *controller,
                      const UpdateSchemaRequest *request,
                      UpdateSchemaResponse *response,
                      google::protobuf::Closure *done) override;

    void getSchemaVersion(google::protobuf::RpcController *controller,
                          const GetSchemaVersionRequest *request,
                          GetSchemaVersionResponse *response,
                          google::protobuf::Closure *done) override;

private:
    bool initSearchService(const suez::ServiceInfo &serviceInfo);
    void setSearchService(const multi_call::SearchServicePtr &newService);
    multi_call::SearchServicePtr getSearchService() const;
    autil::legacy::json::JsonArray extractCm2Configs(const suez::ServiceInfo &serviceInfo);
    void createMultiCallConfig(const autil::legacy::json::JsonArray &cm2Configs,
                               multi_call::MultiCallConfig *config) const;
    void addMultiCallSubConfig(const autil::legacy::json::JsonArray &cm2Configs,
                               multi_call::MultiCallConfig *config) const;
    bool getLeaderRouter(const std::string &zoneName, std::shared_ptr<LeaderRouter> &router);

private:
    std::unique_ptr<kmonitor::MetricsReporter> _metricsReporter;
    std::unordered_map<std::string, std::shared_ptr<LeaderRouter>> _leaderRouterMap;
    mutable autil::ReadWriteLock _lock;
    mutable autil::ThreadMutex _searchServiceMutex;
    multi_call::SearchServicePtr _searchService;
};

} // namespace suez
