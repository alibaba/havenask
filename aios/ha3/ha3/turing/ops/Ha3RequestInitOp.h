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
#include <iostream>

#include "autil/Log.h" // IWYU pragma: keep
#include "suez/turing/common/SessionResource.h"
#include "tensorflow/core/framework/op_kernel.h"

#include "ha3/common/TimeoutTerminator.h"
#include "ha3/common/Tracer.h"
#include "ha3/proto/ProtoClassUtil.h"
#include "ha3/turing/ops/SearcherQueryResource.h"
#include "ha3/turing/ops/SearcherSessionResource.h"
#include "ha3/turing/variant/Ha3RequestVariant.h"
#include "ha3/turing/variant/Ha3ResultVariant.h"
#include "autil/Log.h" // IWYU pragma: keep

namespace isearch {
namespace turing {

class Ha3RequestInitOp : public tensorflow::OpKernel
{
public:
    explicit Ha3RequestInitOp(tensorflow::OpKernelConstruction* ctx) : tensorflow::OpKernel(ctx) {};
public:
    void Compute(tensorflow::OpKernelContext* ctx) override;

private:
    void addEagleTraceInfo(multi_call::QuerySession *querySession, isearch::common::Tracer *tracer) ;
    isearch::common::TimeoutTerminatorPtr createTimeoutTerminator(const isearch::common::RequestPtr &request,
            int64_t startTime, int64_t currentTime, bool isSeekTimeout);

private:
    static const int32_t MS_TO_US_FACTOR = 1000;
    AUTIL_LOG_DECLARE();
};


} // namespace turing
} // namespace isearch
