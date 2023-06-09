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

#include <string>

#include "autil/Log.h" // IWYU pragma: keep
#include "tensorflow/core/framework/variant_encode_decode.h" // IWYU pragma: keep
#include "tensorflow/core/framework/variant_tensor_data.h"

#include "ha3/common/Result.h"

namespace autil {
namespace mem_pool {
class Pool;
}  // namespace mem_pool
}  // namespace autil


namespace isearch {
namespace turing {
class Ha3ResultVariant
{
public:
    Ha3ResultVariant();
    Ha3ResultVariant(autil::mem_pool::Pool *pool);
    Ha3ResultVariant(isearch::common::ResultPtr result, autil::mem_pool::Pool *pool);
    Ha3ResultVariant(const Ha3ResultVariant &other);
    ~Ha3ResultVariant();
public:
    void Encode(tensorflow::VariantTensorData* data) const;
    bool Decode(const tensorflow::VariantTensorData& data);
    std::string TypeName() const {
        return "Ha3Result";
    }
    isearch::common::ResultPtr getResult() const { return _result; }
    bool construct(autil::mem_pool::Pool *outPool);
private:
    autil::mem_pool::Pool* _externalPool;
    isearch::common::ResultPtr _result;
    std::string _metadata;
private:
    AUTIL_LOG_DECLARE();
};

} // namespace turing
} // namespace isearch
