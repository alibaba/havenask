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
#include <iosfwd>
#include <memory>
#include <string>
#include <vector>

#include "alog/Logger.h"
#include "autil/Log.h"
#include "autil/RangeUtil.h"
#include "autil/TimeUtility.h"
#include "ha3/common/AggregateResult.h"
#include "ha3/common/CommonDef.h"
#include "ha3/common/ErrorDefine.h"
#include "ha3/common/ErrorResult.h"
#include "ha3/common/MatchDocs.h"
#include "ha3/common/Result.h"
#include "ha3/turing/searcher/BasicSearcherContext.h"
#include "ha3/turing/variant/Ha3ResultVariant.h"
#include "aios/network/gig/multi_call/common/ErrorCode.h"
#include "suez/sdk/ServiceInfo.h"
#include "suez/turing/common/QueryResource.h"
#include "suez/turing/common/RuntimeState.h"
#include "suez/turing/common/SessionResource.h"
#include "suez/turing/common/TuringRequest.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/proto/ErrorCode.pb.h"
#include "suez/turing/proto/Search.pb.h"
#include "suez/turing/search/GraphSearchContext.h"
#include "tensorflow/core/framework/tensor.h"
#include "tensorflow/core/framework/tensor_shape.h"
#include "tensorflow/core/framework/tensor_types.h"
#include "tensorflow/core/framework/types.pb.h"
#include "tensorflow/core/framework/variant.h"
#include "tensorflow/core/lib/core/error_codes.pb.h"

namespace suez {
namespace turing {
struct SearchContextArgs;
}  // namespace turing
}  // namespace suez

using namespace std;
using namespace suez::turing;
using namespace autil;
using namespace tensorflow;

using namespace isearch::common;
using namespace isearch::util;

namespace isearch {
namespace turing {
AUTIL_LOG_SETUP(ha3, BasicSearcherContext);

BasicSearcherContext::BasicSearcherContext(const SearchContextArgs &args,
        const GraphRequest *request,
        GraphResponse *response)
    : GraphSearchContext(args, request, response)
{
}

BasicSearcherContext::~BasicSearcherContext() {
}

void BasicSearcherContext::formatErrorResult() {
    _graphResponse->set_multicall_ec(multi_call::MULTI_CALL_REPLY_ERROR_RESPONSE);
    const vector<string> &outputNames = _request->outputNames;
    if (outputNames.size() != 1 || outputNames[0] != HA3_RESULT_TENSOR_NAME) {
        return;
    }
    common::ResultPtr result = common::ResultPtr(new common::Result(new common::MatchDocs()));
    ErrorResult errResult(ERROR_RUN_SEARCHER_GRAPH_FAILED, "run searcher graph failed: " +
                          _errorInfo.errormsg());
    result->addErrorResult(errResult);
    if (_queryResource->getTracerPtr()) {
        result->setTracer(_queryResource->getTracerPtr());
    }

    const suez::ServiceInfo &serviceInfo = _sessionResource->serviceInfo;
    string zoneBizName = serviceInfo.getZoneName()  + "." + _sessionResource->bizName;
    PartitionRange utilRange;
    if (!RangeUtil::getRange(serviceInfo.getPartCount(), serviceInfo.getPartId(), utilRange)) {
        AUTIL_LOG(ERROR, "get Range failed");
    }
    result->addCoveredRange(zoneBizName, utilRange.first, utilRange.second);

    auto resultTensor = Tensor(DT_VARIANT, TensorShape({}));
    Ha3ResultVariant ha3Result(result, _pool);
    resultTensor.scalar<Variant>()() = ha3Result;
    suez::turing::NamedTensorProto *namedTensor = _graphResponse->add_outputs();
    namedTensor->set_name(HA3_RESULT_TENSOR_NAME);
    resultTensor.AsProtoField(namedTensor->mutable_tensor());
}

} // namespace turing
} // namespace isearch
