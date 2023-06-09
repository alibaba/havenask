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

#include <stdint.h>
#include <deque>
#include <utility>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/search/LayerMetas.h"
#include "tensorflow/core/framework/op_kernel.h"

namespace autil {
namespace mem_pool {
class Pool;
}  // namespace mem_pool
}  // namespace autil

namespace isearch {
namespace turing {
class RangeSplitOp : public tensorflow::OpKernel {
public:
    explicit RangeSplitOp(tensorflow::OpKernelConstruction* ctx);
public:
    void Compute(tensorflow::OpKernelContext* ctx) override;
public:
    static std::vector<search::LayerMetasPtr> splitLayer(
            const search::LayerMetasPtr &layerMetas,
            int count, autil::mem_pool::Pool *pool);
    static search::LayerMetasPtr splitLayerMeta(
            std::deque<std::pair<int32_t, int32_t> > &ranges,
            int32_t capacity,
            autil::mem_pool::Pool *pool);
    static void updateQuota(std::vector<search::LayerMetasPtr> &layerMetasVec);
    static bool mergeLayerMeta(const search::LayerMetasPtr &layerMetas, search::LayerMeta &layerMeta);
private:
    int32_t _outputNum;
private:
    AUTIL_LOG_DECLARE();
};

} // namespace turing
} // namespace isearch

