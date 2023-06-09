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

#include <iostream>

#include "autil/Log.h" // IWYU pragma: keep
#include "suez/turing/common/SessionResource.h"
#include "tensorflow/core/framework/op_kernel.h"

#include "ha3/search/MatchDocSearcher.h"
#include "ha3/service/SearcherResource.h"
#include "ha3/turing/ops/SearcherQueryResource.h"
#include "ha3/turing/ops/SearcherSessionResource.h"
#include "ha3/turing/variant/Ha3RequestVariant.h"
#include "autil/Log.h" // IWYU pragma: keep

namespace isearch {
namespace turing {

class Ha3ReleaseRedundantV2Op : public tensorflow::OpKernel
{
public:
    explicit Ha3ReleaseRedundantV2Op(tensorflow::OpKernelConstruction* ctx) : tensorflow::OpKernel(ctx) {
    }
public:

    void Compute(tensorflow::OpKernelContext* ctx) override;

private:
    void releaseRedundantMatchDocs(std::vector<matchdoc::MatchDoc> &matchDocVec,
                                   uint32_t resultCount,
                                   const common::Ha3MatchDocAllocatorPtr &matchDocAllocator);
    uint32_t getResultCount(const common::Request *request, uint32_t requiredTopK, SearcherQueryResource *searcherQueryResource);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace turing
} // namespace isearch
