#include <ha3/turing/qrs/TuringUrlTransform.h>
#include <map>
#include <memory>
#include <sap/util/environ_util.h>
#include <autil/ConstString.h>
#include <autil/StringTokenizer.h>
#include <ha3/common/ha3_op_common.h>

using namespace std;
using namespace sap;
using namespace autil;
using namespace tensorflow;
using namespace suez::turing;

BEGIN_HA3_NAMESPACE(turing);
HA3_LOG_SETUP(turing, TuringUrlTransform);

bool TuringUrlTransform::init() {
    auto bizNameKey = EnvironUtil::getEnv("TURING_BIZ_NAME_FIELD", "");
    if (! bizNameKey.empty()) {
        _bizNameKey = bizNameKey;
    }
    auto srcKey = EnvironUtil::getEnv("TURING_SRC_FIELD", "");
    if (! srcKey.empty()) {
        _srcKey = srcKey;
    }
    return true;
}

suez::turing::GraphRequest* TuringUrlTransform::transform(const std::string& url) const {
    KvPairVariant kvPairVariant;
    auto& kvpairs = kvPairVariant.get();

    ConstString rawUrl(url.c_str(), url.size());
    auto params = StringTokenizer::constTokenize(rawUrl, '&');
    for (const auto& param : params) {
        auto pos = param.find('=');
        if (pos == string::npos || pos == 0u) {
            continue;
        }
        auto key = param.subString(0, pos).toString();
        auto value = param.subString(pos + 1).toString();
        kvpairs.insert({key, value});
    }

    unique_ptr<GraphRequest> pRequest(new GraphRequest);
    auto iter = kvpairs.find(_bizNameKey);
    if (iter == kvpairs.end() || iter->second.empty()) {
        HA3_LOG(WARN, "bizName key [%s] not found in url", _bizNameKey.c_str());
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

END_HA3_NAMESPACE(turing);
