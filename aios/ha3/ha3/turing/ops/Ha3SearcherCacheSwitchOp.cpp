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
#include <stdint.h>
#include <iosfwd>
#include <memory>

#include "autil/mem_pool/MemoryChunk.h"
#include "turing_ops_util/util/OpUtil.h"
#include "suez/turing/common/QueryResource.h"
#include "suez/turing/common/RuntimeState.h"
#include "suez/turing/common/SessionResource.h"
#include "tensorflow/core/framework/kernel_def_builder.h"
#include "tensorflow/core/framework/op_kernel.h"
#include "tensorflow/core/framework/tensor_shape.pb.h"
#include "tensorflow/core/framework/types.h"
#include "tensorflow/core/lib/core/errors.h"
#include "tensorflow/core/lib/core/stringpiece.h"

#include "ha3/common/Tracer.h"
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "ha3/search/SearcherCacheInfo.h"
#include "ha3/turing/ops/SearcherQueryResource.h"
#include "autil/Log.h" // IWYU pragma: keep

using namespace std;
using namespace tensorflow;
using namespace autil::mem_pool;

using namespace isearch::search;
namespace isearch {
namespace turing {

class Ha3SearcherCacheSwitchOp : public tensorflow::OpKernel
{
public:
    explicit Ha3SearcherCacheSwitchOp(tensorflow::OpKernelConstruction* ctx)
        : tensorflow::OpKernel(ctx)
    {}
public:
    void Compute(tensorflow::OpKernelContext* ctx) override {
        auto sessionResource = GET_SESSION_RESOURCE();
        auto queryResource = GET_QUERY_RESOURCE(sessionResource);

        SearcherQueryResource *searcherQueryResource =
            dynamic_cast<SearcherQueryResource *>(queryResource);
        OP_REQUIRES(ctx, searcherQueryResource,
                    errors::Unavailable("ha3 searcher query resource unavailable"));
        auto cacheInfo = searcherQueryResource->searcherCacheInfo;
        int32_t outPort = 0;
        if (cacheInfo) {
            if (!cacheInfo->isHit) {
                outPort = 1;
            } else {
                outPort = 2;
            }
        }
        REQUEST_TRACE_WITH_TRACER(queryResource->getTracer(), TRACE2, "switch port %d", outPort);
        ctx->set_output(outPort, ctx->input(0));
    }

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(ha3, Ha3SearcherCacheSwitchOp);

REGISTER_KERNEL_BUILDER(Name("Ha3SearcherCacheSwitchOp")
                        .Device(DEVICE_CPU),
                        Ha3SearcherCacheSwitchOp);

} // namespace turing
} // namespace isearch
