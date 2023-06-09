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
#include <set>
#include <string>

#include "autil/Log.h"
#include "ha3/sql/common/Log.h" // IWYU pragma: keep
#include "suez/turing/search/base/SearchManagerAdapter.h"

namespace google {
namespace protobuf {
class Closure;
class RpcController;
} // namespace protobuf
} // namespace google
namespace suez {
class RpcServer;

namespace turing {
class ServiceSnapshotBase;
} // namespace turing
} // namespace suez

namespace isearch {
namespace turing {

class SearcherServiceImpl : public suez::turing::SearchManagerAdapter {
public:
    SearcherServiceImpl();
    ~SearcherServiceImpl();

private:
    SearcherServiceImpl(const SearcherServiceImpl &);
    SearcherServiceImpl &operator=(const SearcherServiceImpl &);

protected:
    bool doInit() override;
    std::shared_ptr<suez::turing::ServiceSnapshotBase> doCreateServiceSnapshot() override;
    bool doInitRpcServer(suez::RpcServer *rpcServer) override;
    void beforeStopServiceSnapshot() override;
    void stopWorker() override;
    std::unique_ptr<iquan::CatalogInfo> getCatalogInfo() const override;

private:
    void rewriteWorkerParam();

private:
    bool _enableSql;
    std::set<std::string> _basicTuringBizNames;
    std::string _defaultAggStr;
    std::string _paraWaysStr;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<SearcherServiceImpl> SearcherServiceImplPtr;
} // namespace sql
} // namespace isearch
