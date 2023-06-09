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

#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/common/Request.h"
#include "ha3/common/Result.h"
#include "ha3/common/Tracer.h"
#include "suez/turing/common/QueryResource.h"
#include "suez/sdk/RpcServer.h"
#include "tensorflow/core/protobuf/config.pb.h"

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil
namespace tensorflow {
class Session;
class SessionResource;
class Status;
class Tensor;
} // namespace tensorflow
namespace google {
namespace protobuf {
class Service;
} // namespace protobuf
} // namespace google

static const std::string RPC_SERVEER_FOR_SERACHER = "rpc_server_for_searcher";

namespace isearch {
namespace turing {

struct QrsRunGraphContextArgs {
    tensorflow::Session *session = nullptr;
    const std::vector<std::string> *inputs = nullptr;
    tensorflow::SessionResource *sessionResource = nullptr;
    tensorflow::RunOptions runOptions;
    autil::mem_pool::Pool *pool = nullptr;
};

class QrsRunGraphContext {
public:
    typedef std::function<void(const tensorflow::Status &)> StatusCallback;

public:
    QrsRunGraphContext(const QrsRunGraphContextArgs &args);
    ~QrsRunGraphContext();

private:
    QrsRunGraphContext(const QrsRunGraphContext &);
    QrsRunGraphContext &operator=(const QrsRunGraphContext &);

public:
    void setResults(const common::ResultPtrVector &results) {
        _results = results;
    }
    void setRequest(const common::RequestPtr &request) {
        _request = request;
    }

    void run(std::vector<tensorflow::Tensor> *outputs,
             tensorflow::RunMetadata *runMetadata,
             StatusCallback done);

    tensorflow::SessionResource *getSessionResource() {
        return _sessionResource;
    }
    void setQueryResource(suez::turing::QueryResource *queryResource) {
        _queryResource = queryResource;
    }
    suez::turing::QueryResource *getQueryResource() {
        return _queryResource;
    }
    autil::mem_pool::Pool *getPool() {
        return _sessionPool;
    }
    void setTracer(common::TracerPtr tracer) {
        if (_queryResource) {
            _queryResource->setTracer(tracer);
        }
    }

    void setSharedObject(suez::RpcServer *rpcService);
    suez::RpcServer *getRpcServer();

private:
    bool getInputs(std::vector<std::pair<std::string, tensorflow::Tensor>> &inputs);
    std::vector<std::string> getTargetNodes() const;
    std::vector<std::string> getOutputNames() const;
    void cleanQueryResource();

private:
    common::RequestPtr _request;
    common::ResultPtrVector _results;
    tensorflow::Session *_session = nullptr;
    const std::vector<std::string> *_inputs = nullptr;
    autil::mem_pool::Pool *_sessionPool = nullptr;
    tensorflow::SessionResource *_sessionResource = nullptr;
    suez::turing::QueryResource *_queryResource = nullptr;
    tensorflow::RunOptions _runOptions;

private:
    static const std::string HA3_REQUEST_TENSOR_NAME;
    static const std::string HA3_RESULTS_TENSOR_NAME;
    static const std::string HA3_RESULT_TENSOR_NAME;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<QrsRunGraphContext> QrsRunGraphContextPtr;

} // namespace turing
} // namespace isearch
