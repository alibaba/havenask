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

#include <stddef.h>
#include <stdint.h>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/index/partition_info.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/misc/common.h"
#include "indexlib/partition/index_partition_reader.h"
#include "indexlib/partition/partition_reader_snapshot.h"
#include "suez/turing/expression/cava/common/CavaJitWrapper.h"
#include "suez/turing/expression/cava/common/CavaPluginManager.h"
#include "suez/turing/expression/framework/AttributeExpressionCreator.h"
#include "suez/turing/expression/util/TableInfo.h"
#include "suez/turing/expression/util/VirtualAttribute.h"

#include "ha3/common/ConfigClause.h"
#include "ha3/common/DataProvider.h"
#include "ha3/common/GlobalVariableManager.h"
#include "ha3/common/Ha3MatchDocAllocator.h"
#include "ha3/common/MultiErrorResult.h"
#include "ha3/common/Request.h"
#include "ha3/common/TimeoutTerminator.h"
#include "ha3/common/Tracer.h"
#include "ha3/common/VirtualAttributeClause.h"
#include "ha3/func_expression/FunctionProvider.h"
#include "ha3/isearch.h"
#include "ha3/rank/ComparatorCreator.h"
#include "ha3/rank/GlobalMatchData.h"
#include "ha3/search/DocCountLimits.h"
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "ha3/search/MatchDataManager.h"
#include "ha3/search/SortExpressionCreator.h"
#include "autil/Log.h" // IWYU pragma: keep

namespace autil {
namespace mem_pool {
class Pool;
}  // namespace mem_pool
}  // namespace autil
namespace isearch {
namespace monitor {
class SessionMetricsCollector;
}  // namespace monitor
namespace rank {
class RankProfile;
}  // namespace rank
}  // namespace isearch
namespace suez {
namespace turing {
class FunctionInterfaceCreator;
class SuezCavaAllocator;
}  // namespace turing
}  // namespace suez

namespace isearch {
namespace search {

class SearchCommonResource
{
public:
    SearchCommonResource(
            autil::mem_pool::Pool *pool_,
            const suez::turing::TableInfoPtr &tableInfoPtr_,
            monitor::SessionMetricsCollector *sessionMetricsCollector_,
            common::TimeoutTerminator *timeoutTerminator_,
            common::Tracer* tracer_,
            const suez::turing::FunctionInterfaceCreator *funcCreator_,
            const suez::turing::CavaPluginManager *cavaPluginManager_,
            const common::Request *request,
            suez::turing::SuezCavaAllocator *cavaAllocator_,
            cava::CavaJitModulesWrapper *cavaJitModulesWrapper_,
            common::Ha3MatchDocAllocatorPtr matchDocAllocator_ =
            common::Ha3MatchDocAllocatorPtr())
        : pool(pool_)
        , tableInfoPtr(tableInfoPtr_)
        , tableInfo(tableInfoPtr.get())
        , sessionMetricsCollector(sessionMetricsCollector_)
        , timeoutTerminator(timeoutTerminator_)
        , tracer(tracer_)
        , functionCreator(funcCreator_)
        , cavaPluginManager(cavaPluginManager_)
        , errorResult(new common::MultiErrorResult())
        , dataProvider()
        , cavaAllocator(cavaAllocator_)
        , cavaJitModulesWrapper(cavaJitModulesWrapper_)
    {
        if (matchDocAllocator_) {
            matchDocAllocator = matchDocAllocator_;
        } else {
            matchDocAllocator.reset(new common::Ha3MatchDocAllocator(
                            pool, useSubDoc(request, tableInfo)));
        }
    }

    ~SearchCommonResource() {
    }
private:
    static bool useSubDoc(const common::Request *request, const suez::turing::TableInfo *tableInfo) {
        if (!request) {
            return false;
        }
        common::ConfigClause *configClause = request->getConfigClause();
        if (configClause != NULL
            && configClause->getSubDocDisplayType() != SUB_DOC_DISPLAY_NO
            && tableInfo && tableInfo->hasSubSchema())
        {
            return true;
        }
        return false;
    }
public:
    autil::mem_pool::Pool *pool = nullptr;
    suez::turing::TableInfoPtr tableInfoPtr;
    const suez::turing::TableInfo *tableInfo = nullptr;
    monitor::SessionMetricsCollector *sessionMetricsCollector = nullptr;
    common::TimeoutTerminator *timeoutTerminator = nullptr;
    common::Tracer* tracer = nullptr;
    const suez::turing::FunctionInterfaceCreator *functionCreator = nullptr;
    const suez::turing::CavaPluginManager *cavaPluginManager;
    common::MultiErrorResultPtr errorResult;
    common::Ha3MatchDocAllocatorPtr matchDocAllocator;
    common::DataProvider dataProvider;
    suez::turing::SuezCavaAllocator *cavaAllocator = nullptr;
    cava::CavaJitModulesWrapper *cavaJitModulesWrapper = nullptr;
    std::vector<common::GlobalVariableManagerPtr> paraGlobalVariableManagers;
};
typedef std::shared_ptr<SearchCommonResource> SearchCommonResourcePtr;

class SearchPartitionResource
{
public:
    SearchPartitionResource(
            SearchCommonResource& resource,
            const common::Request *request,
            const IndexPartitionReaderWrapperPtr &indexPartitionReaderWrapper_,
            indexlib::partition::PartitionReaderSnapshot *partitionReaderSnapshot_)
        : mainTable(indexPartitionReaderWrapper_ ?
                    indexPartitionReaderWrapper_->getSchema()->GetTableName() : "")
        , indexPartitionReaderWrapper(indexPartitionReaderWrapper_)
        , partitionReaderSnapshot(partitionReaderSnapshot_)
        , functionProvider(createFunctionResource(resource, request))
        , attributeExpressionCreator(new suez::turing::AttributeExpressionCreator(
                        resource.pool,
                        resource.matchDocAllocator.get(),
                        mainTable,
                        partitionReaderSnapshot,
                        resource.tableInfoPtr,
                        (request && request->getVirtualAttributeClause()
                                ? request->getVirtualAttributeClause()->getVirtualAttributes()
                                : suez::turing::VirtualAttributes()),
                        resource.functionCreator,
                        resource.cavaPluginManager,
                        &functionProvider))
    {
    }
    // for test
    SearchPartitionResource()
        : functionProvider({})
    {
    }
    ~SearchPartitionResource() {
        attributeExpressionCreator.reset();
    }

    int32_t getTotalDocCount() const {
        if (indexPartitionReaderWrapper) {
            return indexPartitionReaderWrapper->getPartitionInfo()->GetTotalDocCount();
        }
        return 0;
    }

private:
    SearchPartitionResource(const SearchPartitionResource &);
    SearchPartitionResource& operator=(const SearchPartitionResource &);
private:
    func_expression::FunctionResource createFunctionResource(
            SearchCommonResource& resource,
            const common::Request *request)
    {
        func_expression::FunctionResource funcResource;
        funcResource.pool = resource.pool;
        funcResource.request = request;
        funcResource.globalMatchData.setDocCount(getTotalDocCount());
        funcResource.matchDataManager = &matchDataManager;
        funcResource.attrExprCreator = attributeExpressionCreator.get();
        funcResource.dataProvider = &resource.dataProvider;
        funcResource.matchDocAllocator = resource.matchDocAllocator;
        funcResource.fieldInfos = resource.tableInfo ? resource.tableInfo->getFieldInfos() : NULL;
        funcResource.queryTerminator = resource.timeoutTerminator;
        funcResource.requestTracer = resource.tracer;
        funcResource.cavaAllocator = resource.cavaAllocator;
        funcResource.kvpairs = request && request->getConfigClause() ?
                               &request->getConfigClause()->getKVPairs() : NULL;
        funcResource.partitionReaderSnapshot = partitionReaderSnapshot;

        return funcResource;
    }

public:
    std::string mainTable;
    IndexPartitionReaderWrapperPtr indexPartitionReaderWrapper;
    indexlib::partition::PartitionReaderSnapshot *partitionReaderSnapshot = nullptr;
    MatchDataManager matchDataManager;

    func_expression::FunctionProvider functionProvider;
    suez::turing::AttributeExpressionCreatorPtr attributeExpressionCreator;
};

typedef std::shared_ptr<SearchPartitionResource> SearchPartitionResourcePtr;

struct SearchRuntimeResource {
    SortExpressionCreatorPtr sortExpressionCreator;
    rank::ComparatorCreatorPtr comparatorCreator;
    DocCountLimits docCountLimits;
    const rank::RankProfile *rankProfile = nullptr;
};

typedef std::shared_ptr<SearchRuntimeResource> SearchRuntimeResourcePtr;
} // namespace search
} // namespace isearch
