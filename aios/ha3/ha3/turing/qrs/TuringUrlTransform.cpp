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
#include "ha3/turing/qrs/TuringUrlTransform.h"

#include <iosfwd>
#include <map>
#include <memory>
#include <utility>
#include <vector>

#include "alog/Logger.h"
#include "autil/ConstString.h"
#include "autil/EnvUtil.h"
#include "autil/Log.h"
#include "autil/StringTokenizer.h"
#include "autil/TimeUtility.h"
#include "suez/turing/proto/Search.pb.h"
#include "tensorflow/core/framework/resource_handle.pb.h"
#include "tensorflow/core/framework/tensor.h"
#include "tensorflow/core/framework/tensor_shape.h"
#include "tensorflow/core/framework/tensor_types.h"
#include "tensorflow/core/framework/types.pb.h"
#include "tensorflow/core/framework/variant.h"
#include "turing_ops_util/variant/KvPairVariant.h"


using namespace std;
using namespace autil;
using namespace tensorflow;
using namespace suez::turing;

namespace isearch {
namespace turing {
AUTIL_LOG_SETUP(ha3, TuringUrlTransform);

bool TuringUrlTransform::init() {
    auto bizNameKey = EnvUtil::getEnv("TURING_BIZ_NAME_FIELD", "");
    if (! bizNameKey.empty()) {
        _bizNameKey = bizNameKey;
    }
    auto srcKey = EnvUtil::getEnv("TURING_SRC_FIELD", "");
    if (! srcKey.empty()) {
        _srcKey = srcKey;
    }
    return true;
}

suez::turing::GraphRequest* TuringUrlTransform::transform(const std::string& url) const {
    KvPairVariant kvPairVariant;
    auto& kvpairs = kvPairVariant.get();

    StringView rawUrl(url.c_str(), url.size());
    auto params = StringTokenizer::constTokenize(rawUrl, '&');
    for (const auto& param : params) {
        auto pos = param.find('=');
        if (pos == string::npos || pos == 0u) {
            continue;
        }
        auto key = param.substr(0, pos).to_string();
        auto value = param.substr(pos + 1).to_string();
        kvpairs.insert({key, value});
    }

    unique_ptr<GraphRequest> pRequest(new GraphRequest);
    auto iter = kvpairs.find(_bizNameKey);
    if (iter == kvpairs.end() || iter->second.empty()) {
        AUTIL_LOG(WARN, "bizName key [%s] not found in url", _bizNameKey.c_str());
        return nullptr;
    }
    pRequest->set_bizname(iter->second);
    pRequest->set_timeout(_timeout);
    iter = kvpairs.find(_srcKey);
    if (iter != kvpairs.end()) {
        pRequest->set_src(iter->second);
    }

    GraphInfo* pGraphInfo = pRequest->mutable_graphinfo();
    pGraphInfo->add_fetches()->assign("format_result");
    pGraphInfo->add_targets()->assign("run_done");

    suez::turing::NamedTensorProto* pNamedTensorProto = pGraphInfo->add_inputs();
    pNamedTensorProto->set_name("query_kv_pair");
    auto inputTensor = Tensor(DT_VARIANT, TensorShape({}));
    inputTensor.scalar<Variant>()() = kvPairVariant;
    inputTensor.AsProtoField(pNamedTensorProto->mutable_tensor());

    return pRequest.release();
}

void TuringUrlTransform::setBizNameKey(const std::string& key) {
    _bizNameKey = key;
}

const std::string& TuringUrlTransform::getBizNameKey() const {
    return _bizNameKey;
}

void TuringUrlTransform::setSrcKey(const std::string& key) {
    _srcKey = key;
}

const std::string& TuringUrlTransform::getSrcKey() const {
    return _srcKey;
}

void TuringUrlTransform::setTimeout(int64_t t) {
    _timeout = t;
}

int64_t TuringUrlTransform::getTimeout() const {
    return _timeout;
}

} // namespace turing
} // namespace isearch
