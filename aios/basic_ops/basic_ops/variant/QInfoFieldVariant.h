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

#include <map>
#include <string>

#include "autil/Log.h"
#include "autil/mem_pool/Pool.h"
#include "tensorflow/core/framework/variant_tensor_data.h"

#include "basic_ops/common/RapidJsonCommon.h"

namespace suez {
namespace turing {

class QInfoFieldVariant
{
public:
    QInfoFieldVariant();
    QInfoFieldVariant(const QInfoFieldVariant &other);
public:
    // for tensorflow::Variant protocol
    void Encode(tensorflow::VariantTensorData* data) const;
    bool Decode(const tensorflow::VariantTensorData& data);
    // hack, compatible with rtp
    bool Decode(const std::string &data);
    std::string TypeName() const {
        return "QinfoFieldType";
    }
    const SimpleDocument *getQuery() const {
        return _query;
    }
    void set(const SimpleDocument *query) {
        _query = query;
    }
    std::string DebugString() const;
    void copyFromKvMap(const std::map<std::string, std::string> &kv_pair);
private:
    std::string toJsonString() const;
private:
    const SimpleDocument *_query;
    autil::mem_pool::Pool _pool;
    AutilPoolAllocator _allocator;
    SimpleDocument _decodeQuery;
    bool _initFromDecode;
    AUTIL_LOG_DECLARE();
};

}
}
