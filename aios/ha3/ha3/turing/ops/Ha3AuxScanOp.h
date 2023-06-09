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

#include <memory>
#include <string>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/common/FilterClause.h"
#include "ha3/search/FilterWrapper.h"
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "ha3/search/LayerMetas.h"
#include "ha3/search/QueryExecutor.h"
#include "ha3/common/Query.h"
#include "kmonitor/client/core/MetricsTags.h"
#include "tensorflow/core/framework/op_kernel.h"

namespace autil {
namespace mem_pool {
class Pool;
}  // namespace mem_pool
}  // namespace autil
namespace matchdoc {
class MatchDocAllocator;
}  // namespace matchdoc
namespace suez {
namespace turing {
class AttributeExpressionCreator;
class Tracer;
}  // namespace turing
}  // namespace suez

namespace isearch {
namespace turing {

class Ha3AuxScanOp : public tensorflow::OpKernel
{
public:
    explicit Ha3AuxScanOp(tensorflow::OpKernelConstruction* ctx);
    ~Ha3AuxScanOp();
private:
    Ha3AuxScanOp(const Ha3AuxScanOp &);
    Ha3AuxScanOp& operator=(const Ha3AuxScanOp &);
public:
    void Compute(tensorflow::OpKernelContext* ctx) override;
private:
    isearch::search::FilterWrapperPtr createFilterWrapper(
        const isearch::common::AuxFilterClause *auxFilterClause,
        suez::turing::AttributeExpressionCreator *attrExprCreator,
        matchdoc::MatchDocAllocator *matchDocAllocator,
	autil::mem_pool::Pool *pool);

    isearch::search::LayerMetaPtr createLayerMeta(
        isearch::search::IndexPartitionReaderWrapperPtr &indexPartitionReader,
        autil::mem_pool::Pool *pool);
    isearch::search::QueryExecutor *createQueryExecutor(const isearch::common::QueryPtr &query,
            isearch::search::IndexPartitionReaderWrapperPtr &indexPartitionReader,
            const std::string &tableName,
            autil::mem_pool::Pool *pool,
            autil::TimeoutTerminator *timeoutTerminator,
            isearch::search::LayerMeta *layerMeta,
            suez::turing::Tracer *tracer,
            bool &emptyScan);

private:
    std::string _tableName;
    std::string _joinFieldName;
    kmonitor::MetricsTags _tags;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<Ha3AuxScanOp> Ha3AuxScanOpPtr;

} // namespace turing
} // namespace isearch
