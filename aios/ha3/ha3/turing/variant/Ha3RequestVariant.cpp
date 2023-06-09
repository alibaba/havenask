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
#include "ha3/turing/variant/Ha3RequestVariant.h"

#include <iosfwd>
#include <memory>

#include "autil/Log.h"
#include "autil/TimeUtility.h"
#include "autil/mem_pool/MemoryChunk.h"
#include "ha3/common/ClauseBase.h"
#include "ha3/common/Request.h"
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

AUTIL_LOG_SETUP(ha3, Ha3RequestVariant);

Ha3RequestVariant::Ha3RequestVariant()
{
    _externalPool = nullptr;
}

Ha3RequestVariant::Ha3RequestVariant(autil::mem_pool::Pool* pool)
{
    _externalPool = pool;
    _request.reset(new Request(_externalPool));
}

Ha3RequestVariant::Ha3RequestVariant(isearch::common::RequestPtr request,
                                     autil::mem_pool::Pool *pool)
{
    _externalPool = pool;
    _request = request;
}

Ha3RequestVariant::Ha3RequestVariant(const Ha3RequestVariant &other)
    : _externalPool(other._externalPool)
    , _request(other._request)
    , _metadata(other._metadata)
{
}

Ha3RequestVariant::~Ha3RequestVariant() {
    _request.reset();
    _externalPool = nullptr;
}

void Ha3RequestVariant::Encode(VariantTensorData* data) const {
    data->set_type_name(TypeName());
    _request->serializeToString(data->metadata_);
}

bool Ha3RequestVariant::Decode(const VariantTensorData& data) {
    if (TypeName() != data.type_name()) {
        return false;
    }
    _metadata = data.metadata_;
    return true;
}

bool Ha3RequestVariant::construct(autil::mem_pool::Pool *outPool) {
    _request.reset(new Request(outPool));
    _request->deserializeFromString(_metadata, outPool);
    return true;
}


REGISTER_UNARY_VARIANT_DECODE_FUNCTION(Ha3RequestVariant, "Ha3Request");

} // namespace turing
} // namespace isearch
