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
#include "basic_ops/variant/QInfoFieldVariant.h"

#include <iosfwd>
#include <memory>

#include "alog/Logger.h"
#include "rapidjson/document.h"
#include "rapidjson/error/error.h"
#include "rapidjson/rapidjson.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"
#include "tensorflow/core/framework/variant_op_registry.h"
#include "tensorflow/core/platform/byte_order.h"

#include "turing_ops_util/util/ParamConverter.h"

using namespace std;
using namespace tensorflow;

namespace suez {
namespace turing {

AUTIL_LOG_SETUP(turing, QInfoFieldVariant);

QInfoFieldVariant::QInfoFieldVariant()
        : _query(nullptr)
        , _pool(1024)
        , _allocator(&_pool)
        , _decodeQuery(&_allocator)
        , _initFromDecode(false)
{
    _decodeQuery.SetObject();
}

QInfoFieldVariant::QInfoFieldVariant(const QInfoFieldVariant &other)
        : _query(nullptr)
        , _pool(1024)
        , _allocator(&_pool)
        , _decodeQuery(&_allocator)
        , _initFromDecode(other._initFromDecode)
{
    _decodeQuery.SetObject();
    // hack, compatible with rtp
    if (!_initFromDecode || other._query == nullptr) {
        _query = other._query;
    } else if (Decode(other.toJsonString())) {
        _query = &_decodeQuery;
    }
}

void QInfoFieldVariant::Encode(VariantTensorData *data) const {
    data->metadata_ = toJsonString();
}

bool QInfoFieldVariant::Decode(const VariantTensorData &data) {
    _query = &_decodeQuery;
    string str(data.metadata_.data(), data.metadata_.size());
    _decodeQuery.Parse(str.c_str());
    if (_decodeQuery.HasParseError()) {
        AUTIL_LOG(ERROR, "parse %s failed", str.c_str());
        return false;
    }
    _initFromDecode = true;
    return true;
}

bool QInfoFieldVariant::Decode(const string &data) {
    if (data.empty()) {
        return false;
    }
    _query = &_decodeQuery;
    _decodeQuery.Parse(data.c_str());
    if (_decodeQuery.HasParseError()) {
        _query = nullptr;
        AUTIL_LOG(ERROR, "parse %s failed", data.c_str());
        return false;
    }
    _initFromDecode = true;
    return true;
}

void QInfoFieldVariant::copyFromKvMap(const map<string, string> &kv_pair) {
    param_converter::extractFgQinfo(kv_pair, _decodeQuery, _allocator);
    _query = &_decodeQuery;
    _initFromDecode = true;
}


std::string QInfoFieldVariant::toJsonString() const {
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    if (_query) {
        _query->Accept(writer);
    } else {
        return "{}";
    }
    return buffer.GetString();
}

std::string QInfoFieldVariant::DebugString() const {
    return toJsonString();
}

REGISTER_UNARY_VARIANT_DECODE_FUNCTION(QInfoFieldVariant, "QinfoFieldType");

}
}
