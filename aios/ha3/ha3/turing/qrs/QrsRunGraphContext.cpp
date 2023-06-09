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
#include "ha3/turing/qrs/QrsRunGraphContext.h"

#include <cstddef>
#include <stdint.h>

#include "autil/Log.h"
#include "ha3/common/ClauseBase.h"
#include "ha3/common/Result.h"
#include "ha3/turing/variant/Ha3RequestVariant.h"
#include "ha3/turing/variant/Ha3ResultVariant.h"
#include "suez/turing/common/RuntimeState.h"
#include "suez/turing/common/SessionResource.h"
#include "tensorflow/core/framework/tensor.h"
#include "tensorflow/core/framework/tensor_shape.h"
#include "tensorflow/core/framework/tensor_shape.pb.h"
#include "tensorflow/core/framework/tensor_types.h"
#include "tensorflow/core/framework/types.pb.h"
#include "tensorflow/core/framework/variant.h"
#include "tensorflow/core/lib/core/errors.h"
#include "tensorflow/core/public/session.h"

using namespace std;
using namespace suez;
using namespace suez::turing;
using namespace tensorflow;

using namespace isearch::common;
namespace isearch {
namespace turing {
AUTIL_LOG_SETUP(ha3, QrsRunGraphContext);
const std::string QrsRunGraphContext::HA3_REQUEST_TENSOR_NAME = "ha3_request";
const std::string QrsRunGraphContext::HA3_RESULTS_TENSOR_NAME = "ha3_results";
const std::string QrsRunGraphContext::HA3_RESULT_TENSOR_NAME = "ha3_result";

QrsRunGraphContext::QrsRunGraphContext(const QrsRunGraphContextArgs &args)
    : _session(args.session)
    , _inputs(args.inputs)
    , _sessionPool(args.pool)
    , _sessionResource(args.sessionResource)
    , _runOptions(args.runOptions) {}

QrsRunGraphContext::~QrsRunGraphContext() {
    cleanQueryResource();
}

void QrsRunGraphContext::run(vector<Tensor> *outputs,
                             RunMetadata *runMetadata,
                             StatusCallback done) {
    vector<pair<string, Tensor>> inputs;
    if (!getInputs(inputs)) {
        done(errors::Internal("prepare inputs failed"));
        return;
    }
    auto targets = getTargetNodes();
    auto outputNames = getOutputNames();
    auto s = _session->Run(_runOptions, inputs, outputNames, targets, outputs, runMetadata);
    done(s);
}

bool QrsRunGraphContext::getInputs(vector<pair<string, Tensor>> &inputs) {
    auto requestTensor = Tensor(DT_VARIANT, TensorShape({}));
    Ha3RequestVariant requestVariant(_request, _sessionPool);
    requestTensor.scalar<Variant>()() = requestVariant;
    inputs.emplace_back(make_pair(HA3_REQUEST_TENSOR_NAME, requestTensor));
    auto resultsTensor = Tensor(DT_VARIANT, TensorShape({(long long int)_results.size()}));
    auto flat = resultsTensor.flat<Variant>();
    for (size_t i = 0; i < _results.size(); i++) {
        Ha3ResultVariant resultVariant(_results[i], _sessionPool);
        flat(i) = resultVariant;
    }
    inputs.emplace_back(make_pair(HA3_RESULTS_TENSOR_NAME, resultsTensor));
    return true;
}

vector<string> QrsRunGraphContext::getTargetNodes() const {
    vector<string> targets;
    targets.push_back(HA3_RESULT_TENSOR_NAME);
    return targets;
}

std::vector<std::string> QrsRunGraphContext::getOutputNames() const {
    vector<string> output;
    output.push_back(HA3_RESULT_TENSOR_NAME);
    return output;
}

void QrsRunGraphContext::setSharedObject(suez::RpcServer *rpcServer) {
    _sessionResource->sharedObjectMap.setWithoutDelete(RPC_SERVEER_FOR_SERACHER, rpcServer);
}

suez::RpcServer *QrsRunGraphContext::getRpcServer() {
    suez::RpcServer *rpcServer = nullptr;
    if (!_sessionResource->sharedObjectMap.get<suez::RpcServer>(RPC_SERVEER_FOR_SERACHER, rpcServer)) {
        return nullptr;
    }
    return rpcServer;
}

void QrsRunGraphContext::cleanQueryResource() {
    int64_t runId = _runOptions.run_id().value();
    if (_sessionResource) {
        _sessionResource->removeQueryResource(runId);
    }
}

} // namespace turing
} // namespace isearch
