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

#include <memory>
#include <mutex>

#include "aios/network/gig/multi_call/common/TopoInfoBuilder.h"
#include "autil/Lock.h"
#include "autil/Log.h"
#include "indexlib/base/proto/query.pb.h"
#include "suez/sdk/SearchManager.h"
#include "suez/service/Service.pb.h"

namespace indexlibv2::config {
class ITabletSchema;
}

namespace kmonitor {
class MetricsReporter;
}

namespace indexlibv2 {
namespace table {
class KVReader;
}
} // namespace indexlibv2

namespace multi_call {
class TopoInfoBuilder;
}

namespace suez {
class TableServiceImpl final : public SearchManager, public TableService {
public:
    TableServiceImpl();
    ~TableServiceImpl();

public:
    // SearchManager interface
    bool init(const SearchInitParam &initParam) override;
    UPDATE_RESULT update(const UpdateArgs &updateArgs,
                         UpdateIndications &updateIndications,
                         SuezErrorCollector &errorCollector) override;
    void stopService() override;
    void stopWorker() override;

    // query interface
    void querySchema(google::protobuf::RpcController *controller,
                     const SchemaQueryRequest *request,
                     SchemaQueryResponse *response,
                     google::protobuf::Closure *done) override;

    void queryVersion(google::protobuf::RpcController *controller,
                      const VersionQueryRequest *request,
                      VersionQueryResponse *response,
                      google::protobuf::Closure *done) override;

    void queryTable(google::protobuf::RpcController *controller,
                    const TableQueryRequest *request,
                    TableQueryResponse *response,
                    google::protobuf::Closure *done) override;

    void simpleQuery(google::protobuf::RpcController *controller,
                     const SimpleQueryRequest *request,
                     SimpleQueryResponse *response,
                     google::protobuf::Closure *done) override;

    void writeTable(google::protobuf::RpcController *controller,
                    const WriteRequest *request,
                    WriteResponse *response,
                    google::protobuf::Closure *done) override;

    void updateSchema(google::protobuf::RpcController *controller,
                      const UpdateSchemaRequest *request,
                      UpdateSchemaResponse *response,
                      google::protobuf::Closure *done) override;

    void getSchemaVersion(google::protobuf::RpcController *controller,
                          const GetSchemaVersionRequest *request,
                          GetSchemaVersionResponse *response,
                          google::protobuf::Closure *done) override;

    void health_check(google::protobuf::RpcController *controller,
                      const HealthCheckRequest *request,
                      HealthCheckResponse *response,
                      google::protobuf::Closure *done) override;

    void kvBatchGet(google::protobuf::RpcController *controller,
                    const KVBatchGetRequest *request,
                    KVBatchGetResponse *response,
                    google::protobuf::Closure *done) override;

private:
    void setIndexProvider(const std::shared_ptr<IndexProvider> &provider);
    std::shared_ptr<IndexProvider> getIndexProvider() const;

    indexlib::Status queryTablet(std::shared_ptr<indexlibv2::framework::ITablet> tablet,
                                 const indexlibv2::base::PartitionQuery &query,
                                 indexlibv2::base::PartitionResponse &partitionResponse,
                                 std::shared_ptr<indexlibv2::config::ITabletSchema> &schema);
    indexlib::Status queryIndexPartition(indexlib::partition::IndexPartitionPtr indexPartitionPtr,
                                         const TableQueryRequest *request,
                                         const indexlibv2::base::PartitionQuery &query,
                                         indexlibv2::base::PartitionResponse &partitionResponse,
                                         std::shared_ptr<indexlibv2::config::ITabletSchema> &schema);

    static indexlibv2::base::PartitionQuery convertRequestToPartitionQuery(const TableQueryRequest *const request);

    static void convertPartitionResponseToResponse(const TableQueryRequest *const request,
                                                   const indexlibv2::base::PartitionResponse &partitionResponse,
                                                   TableQueryResponse *const response);
    static ErrorInfo convertStatusToErrorInfo(const indexlib::Status &status);

    template <typename T>
    static void setSingleAttr(const T *const request,
                              SingleAttrValue &singleAttrValue,
                              const indexlibv2::base::AttrValue &attrValue);
    bool needUpdateTopoInfo(const UpdateArgs &updateArgs) const;
    bool updateTopoInfo(const UpdateArgs &updateArgs);
    bool buildTableTopoInfo(const RpcServer *rpcServer,
                            const UpdateArgs &updateArgs,
                            multi_call::TopoInfoBuilder &infoBuilder);
    bool publishTopoInfo(const RpcServer *rpcServer,
                         const multi_call::PublishGroupTy group,
                         const multi_call::TopoInfoBuilder &infoBuilder) const;

private:
    std::shared_ptr<IndexProvider> _indexProvider;
    mutable autil::ReadWriteLock _lock;
    std::unique_ptr<kmonitor::MetricsReporter> _metricsReporter;
    future_lite::Executor *_executor;
    RpcServer *_rpcServer = nullptr;
    bool _enablePublishTopoInfo = false;

private:
    AUTIL_LOG_DECLARE();
};
} // namespace suez
