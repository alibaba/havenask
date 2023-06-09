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
#include "autil/mem_pool/Pool.h"
#include "suez/turing/common/QueryResource.h"
#include "suez/turing/expression/framework/AttributeExpressionCreator.h"

#include "ha3/common/Request.h"
#include "ha3/common/TimeoutTerminator.h"
#include "ha3/common/Tracer.h"
#include "ha3/isearch.h"
#include "ha3/monitor/SessionMetricsCollector.h"
#include "ha3/search/SearchCommonResource.h"
#include "ha3/service/SearcherResource.h"
#include "autil/Log.h" // IWYU pragma: keep

namespace isearch {
namespace turing {

class Ha3ResourceUtil {
public:
    static isearch::search::SearchCommonResourcePtr createSearchCommonResource(
            const isearch::common::Request *request,
            autil::mem_pool::Pool *pool,
            isearch::monitor::SessionMetricsCollector *collectorPtr,
            isearch::common::TimeoutTerminator *timeoutTerminator,
            isearch::common::Tracer *tracer,
            isearch::service::SearcherResourcePtr &resourcePtr,
            suez::turing::SuezCavaAllocator *cavaAllocator,
            cava::CavaJitModulesWrapper *cavaJitModulesWrapper);

    static isearch::search::SearchPartitionResourcePtr createSearchPartitionResource(
            const isearch::common::Request *request,
            const isearch::search::IndexPartitionReaderWrapperPtr &indexPartReaderWrapperPtr,
            indexlib::partition::PartitionReaderSnapshot *partitionReaderSnapshot,
            isearch::search::SearchCommonResourcePtr &commonResource);
    //TODO remove ptr
    static isearch::search::SearchRuntimeResourcePtr createSearchRuntimeResource(
            const isearch::common::Request *request,
            const isearch::service::SearcherResourcePtr &searcherResource,
            const isearch::search::SearchCommonResourcePtr &commonResource,
            suez::turing::AttributeExpressionCreator *attributeExpressionCreator,
            uint32_t totalPartCount);
private:
    AUTIL_LOG_DECLARE();
};

} // namespace turing
} // namespace isearch
