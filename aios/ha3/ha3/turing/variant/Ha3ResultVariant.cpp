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
#include "ha3/turing/variant/Ha3ResultVariant.h"

#include <iosfwd>
#include <memory>

#include "autil/Log.h"
#include "autil/TimeUtility.h"
#include "autil/mem_pool/MemoryChunk.h"
#include "ha3/common/AggregateResult.h"
#include "ha3/common/Result.h"
#include "tensorflow/core/framework/variant_op_registry.h"
#include "tensorflow/core/platform/byte_order.h"

namespace autil {
namespace mem_pool {
class Pool;
}  // namespace mem_pool
}  // namespace autil

using namespace std;
using namespace tensorflow;
using namespace autil;
using namespace autil::mem_pool;

using namespace isearch::common;
namespace isearch {
namespace turing {

AUTIL_LOG_SETUP(ha3, Ha3ResultVariant);

Ha3ResultVariant::Ha3ResultVariant()
{
    _externalPool = nullptr;
}

Ha3ResultVariant::Ha3ResultVariant(autil::mem_pool::Pool* pool)
{
    _externalPool = pool;
    _result.reset(new common::Result());
}

Ha3ResultVariant::Ha3ResultVariant(isearch::common::ResultPtr result, autil::mem_pool::Pool* pool)
{
    _externalPool = pool;
    _result = result;
}

Ha3ResultVariant::Ha3ResultVariant(const Ha3ResultVariant &other)
    : _externalPool(other._externalPool)
    , _result(other._result)
    , _metadata(other._metadata)
{
}

Ha3ResultVariant::~Ha3ResultVariant() {
    _result.reset();
    _externalPool = nullptr;
}

void Ha3ResultVariant::Encode(VariantTensorData* data) const {
    data->set_type_name(TypeName());
    _result->serializeToString(data->metadata_, _externalPool);
}

bool Ha3ResultVariant::Decode(const VariantTensorData& data) {
    if (TypeName() != data.type_name()) {
        return false;
    }
    _metadata = data.metadata_;
    return true;
}

bool Ha3ResultVariant::construct(autil::mem_pool::Pool *outPool) {
    _result.reset(new common::Result());
    _result->deserializeFromString(_metadata, outPool);
    return true;
}

REGISTER_UNARY_VARIANT_DECODE_FUNCTION(Ha3ResultVariant, "Ha3Result");

} // namespace turing
} // namespace isearch
