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

#include <stdint.h>
#include <memory>
#include <set>
#include <string>

#include "autil/CompressionUtil.h"
#include "autil/Log.h" // IWYU pragma: keep
#include "autil/RangeUtil.h"
#include "ha3/common/Request.h"
#include "ha3/config/QrsConfig.h"
#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/interface/Request.h"
#include "aios/network/gig/multi_call/interface/RequestGenerator.h"
#include "suez/sdk/RpcServer.h"

namespace autil {
namespace mem_pool {
class Pool;
}  // namespace mem_pool
}  // namespace autil
namespace google {
namespace protobuf {
class Arena;
class Service;
}  // namespace protobuf
}  // namespace google
namespace isearch {
namespace common {
class ClusterClause;
class DocIdClause;
}  // namespace common
}  // namespace isearch
namespace suez {
namespace turing {
class GraphRequest;
}  // namespace turing
}  // namespace suez

namespace isearch {
namespace turing {

enum GenerateType {
    GT_NORMAL,
    GT_PID,
    GT_SUMMARY,
};

class Ha3QrsRequestGenerator : public multi_call::RequestGenerator
{
public:
    Ha3QrsRequestGenerator(const std::string &bizName,
                           multi_call::SourceIdTy sourceId,
                           multi_call::VersionTy version,
                           GenerateType type,
                           bool useGrpc,
                           common::RequestPtr requestPtr,
                           int32_t timeout,
                           common::DocIdClause *docIdClause,
                           const config::SearchTaskQueueRule &searchTaskqueueRule,
                           autil::mem_pool::Pool *pool,
                           autil::CompressType compressType,
                           const std::shared_ptr<google::protobuf::Arena> &arena,
                           suez::RpcServer *rpcServer = nullptr);
    ~Ha3QrsRequestGenerator();
private:
    Ha3QrsRequestGenerator(const Ha3QrsRequestGenerator &);
    Ha3QrsRequestGenerator& operator=(const Ha3QrsRequestGenerator &);
    void generate(multi_call::PartIdTy partCnt,
                  multi_call::PartRequestMap &requestMap) override;
private:
    void generateNormal(multi_call::PartIdTy partCnt,
                        multi_call::PartRequestMap &requestMap) const;
    void generatePid(multi_call::PartIdTy partCnt,
                     multi_call::PartRequestMap &requestMap) const;
    void generateSummary(multi_call::PartIdTy partCnt,
                         multi_call::PartRequestMap &requestMap) const;
    void prepareRequest(const std::string &bizName,
                        suez::turing::GraphRequest &graphRequest,
                        bool needClone) const;
    common::RequestPtr getRequest() const {
        return _requestPtr;
    }
    common::ClusterClause *getClusterClause() const {
        return _requestPtr->getClusterClause();
    }
    common::DocIdClause *getDocIdClause() const {
        return _docIdClause;
    }
    void setDocIdClause(common::DocIdClause *docIdClause) {
        _docIdClause = docIdClause;
    }
    multi_call::RequestPtr createQrsRequest(
            const std::string &methodName,
            suez::turing::GraphRequest *graphRequest) const;
private:
    void makeDebugRunOptions(suez::turing::GraphRequest &graphRequest) const;
    std::string getTaskQueueName() const;
private:
    static std::set<int32_t> getPartIds(const autil::RangeVec& ranges,
            const autil::RangeVec& hashIds);
    static void getPartId(const autil::RangeVec& ranges,
                          const autil::PartitionRange &range,
                          std::set<int32_t> &partIds);
private:
    GenerateType _generateType;
    bool _useGrpc;
    common::RequestPtr _requestPtr;
    int32_t _timeout;
    common::DocIdClause *_docIdClause = nullptr;
    config::SearchTaskQueueRule _searchTaskqueueRule;
    autil::mem_pool::Pool *_pool = nullptr;
    autil::CompressType _compressType;
    suez::RpcServer *_rpcServer = nullptr;
private:
    static std::string SEARCH_METHOD_NAME;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<Ha3QrsRequestGenerator> Ha3QrsRequestGeneratorPtr;

} // namespace turing
} // namespace isearch
