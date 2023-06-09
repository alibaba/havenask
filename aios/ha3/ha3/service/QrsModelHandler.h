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
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "autil/mem_pool/Pool.h"
#include "ha3/turing/common/ModelConfig.h"
#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/interface/QuerySession.h"
#include "aios/network/gig/multi_call/interface/Reply.h"
#include "aios/network/gig/multi_call/interface/RequestGenerator.h"
#include "aios/network/gig/multi_call/interface/Response.h"

namespace autil {
namespace mem_pool {
class Pool;
}  // namespace mem_pool
}  // namespace autil
namespace multi_call {
class QuerySession;
}  // namespace multi_call

namespace suez {
namespace turing {
class GraphRequest;
class GraphResponse;
class NamedTensorProto;
class Tracer;
}
}

namespace isearch {
namespace turing {
class ModelConfig;
class ModelInfo;
class NodeConfig;
}  // namespace turing

namespace service {

class QrsModelHandler
{
public:
    QrsModelHandler(int64_t timeout, suez::turing::Tracer *tracer)
        : _timeout(timeout)
        , _tracer(tracer)
    {}
    virtual ~QrsModelHandler() {}
public:
    virtual bool process(const std::vector<std::string> &modelBizs,
                         const std::vector<std::map<std::string, std::string>> &kvPairs,
                         multi_call::SourceIdTy sourceId,
                         std::map<std::string, isearch::turing::ModelConfig> *modelConfigMap,
                         autil::mem_pool::Pool *pool,
                         multi_call::QuerySession *querySession,
                         bool needEncodeResult,
                         std::vector<std::map<std::string, std::string>> &result);
    static bool makeDebugRunOptions(suez::turing::GraphRequest *graphRequest,
                                    const std::string &opDebugStr,
                                    std::string &errorMsg);
private:
    suez::turing::GraphRequest *prepareRequest(
            const std::string &bizName,
            const std::map<std::string, std::string> &kvPairs,
            const isearch::turing::ModelInfo &qrsModelInfo,
            autil::mem_pool::Pool *pool);
    void addBenchMark(multi_call::QuerySession *querySession,
                      const std::string &modelBiz,
                      multi_call::RequestGeneratorPtr generator);
    bool fillResult(const std::vector<std::string> &modelBizs,
                    std::map<std::string, isearch::turing::ModelConfig> *modelConfigMap,
                    const multi_call::ReplyPtr &reply,
                    bool needEncodeResult,
                    std::vector<std::map<std::string, std::string>> &result);
    bool fillResponse(const multi_call::ResponsePtr &response,
                      std::map<std::string, suez::turing::GraphResponse *> &resultMap);
    bool fillOutputTensor(
            const std::vector<std::string> &modelBizs,
            std::map<std::string, isearch::turing::ModelConfig> *modelConfigMap,
            const std::map<std::string, suez::turing::GraphResponse *> &resultMap,
            bool needEncodeResult,
            std::vector<std::map<std::string, std::string>> &result);
    bool addDefaultParam(
            const isearch::turing::NodeConfig &node,
            std::vector<suez::turing::NamedTensorProto> &defaultNamedTensorProto);
private:
    int64_t _timeout;
    suez::turing::Tracer *_tracer;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<QrsModelHandler> QrsModelHandlerPtr;

} // namespace service
} // namespace isearch
