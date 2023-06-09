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
#include "autil/mem_pool/Pool.h"
#include "indexlib/partition/index_partition.h"
#include "suez/turing/common/SessionResource.h"
#include "tensorflow/core/framework/op_kernel.h"

#include "ha3/common/TimeoutTerminator.h"
#include "ha3/common/Tracer.h"
#include "ha3/monitor/SessionMetricsCollector.h"
#include "ha3/proto/ProtoClassUtil.h"
#include "ha3/search/SearchCommonResource.h"
#include "ha3/search/SummarySearcher.h"
#include "ha3/service/SearcherResource.h"
#include "ha3/turing/ops/SearcherQueryResource.h"
#include "ha3/turing/ops/SearcherSessionResource.h"
#include "ha3/turing/variant/Ha3RequestVariant.h"
#include "ha3/turing/variant/Ha3ResultVariant.h"
#include "autil/Log.h" // IWYU pragma: keep
#include "turing_ops_util/util/OpUtil.h"

namespace isearch {
namespace turing {

class Ha3ExtraSummaryOp : public tensorflow::OpKernel
{
public:
    explicit Ha3ExtraSummaryOp(tensorflow::OpKernelConstruction* ctx) : tensorflow::OpKernel(ctx) {
        OP_REQUIRES_OK(ctx, ctx->GetAttr("table_name", &_tableName));

        auto sessionResource = GET_SESSION_RESOURCE();
        auto *searcherSessionResource =
            dynamic_cast<SearcherSessionResource *>(sessionResource);
        OP_REQUIRES(ctx, searcherSessionResource, tensorflow::errors::Unavailable("get searcher session resource failed"));
        auto searcherResource = searcherSessionResource->searcherResource;
        OP_REQUIRES(ctx, searcherResource, tensorflow::errors::Unavailable("get searcher resource failed"));

        if (_tableName.empty()) {
            _tableName = searcherResource->getClusterConfig().getTableName() + "_extra";
        }

        auto tableInfo = sessionResource->dependencyTableInfoMap.find(_tableName);
        if (tableInfo == sessionResource->dependencyTableInfoMap.end()) {
            OP_REQUIRES(ctx, false, tensorflow::errors::Unavailable("find extra table info failed"));
        }

        config::HitSummarySchemaPtr hitSummarySchemaPtr(new config::HitSummarySchema(tableInfo->second.get()));
        config::HitSummarySchemaPtr mainHitSummarySchemaPtr = searcherResource->getHitSummarySchema();
        _schemaConsistent = (hitSummarySchemaPtr->getSignature()
                             == mainHitSummarySchemaPtr->getSignature());

    }
public:
    void Compute(tensorflow::OpKernelContext* ctx) override;
private:
    common::ResultPtr supply(const common::Request *request,
                             const common::ResultPtr& inputResult,
                             service::SearcherResourcePtr resourcePtr,
                             SearcherQueryResource *queryResource,
                             const suez::turing::TableInfoPtr& tableInfo);
    void addTraceInfo(monitor::SessionMetricsCollector *metricsCollector, suez::turing::Tracer* tracer);
    bool needExtraSearch(common::ResultPtr inputResult);
    std::string _tableName;
    bool _schemaConsistent = false;

private:
    AUTIL_LOG_DECLARE();

};
} // namespace turing
} // namespace isearch
