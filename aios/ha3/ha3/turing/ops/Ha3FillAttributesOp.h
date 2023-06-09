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
#include "ha3/rank/RankProfileManager.h"
#include "ha3/search/InnerSearchResult.h"
#include "ha3/search/MatchDocSearcher.h"
#include "ha3/search/SearchCommonResource.h"
#include "ha3/service/SearcherResource.h"
#include "ha3/turing/ops/SearcherQueryResource.h"
#include "ha3/turing/ops/SearcherSessionResource.h"
#include "ha3/turing/variant/AggregateResultsVariant.h"
#include "ha3/turing/variant/Ha3RequestVariant.h"
#include "ha3/turing/variant/Ha3ResultVariant.h"
#include "autil/Log.h" // IWYU pragma: keep

namespace isearch {
namespace turing {

class Ha3FillAttributesOp : public tensorflow::OpKernel
{
public:
    explicit Ha3FillAttributesOp(tensorflow::OpKernelConstruction* ctx) : tensorflow::OpKernel(ctx) {     
    }
public:

    void Compute(tensorflow::OpKernelContext* ctx) override;
    bool fillPk(const common::ConfigClause *configClause,
                std::vector<matchdoc::MatchDoc> &matchDocVec,
                common::Ha3MatchDocAllocatorPtr &matchDocAllocator,
                suez::turing::AttributeExpressionCreator &attributeExpressionCreator,
                search::SearchCommonResourcePtr &commonResource);

    
    bool getPKExprInfo(uint8_t phaseOneInfoMask,
                       std::string &exprName, std::string &refName,
                       search::SearchCommonResourcePtr &commonResource);


    bool fillAttribute(const common::AttributeClause *attributeClause,
                       std::vector<matchdoc::MatchDoc> &matchDocVec,
                       common::Ha3MatchDocAllocatorPtr &matchDocAllocator,
                       suez::turing::AttributeExpressionCreator &attributeExpressionCreator);
private:
    AUTIL_LOG_DECLARE();

};

} // namespace turing
} // namespace isearch
