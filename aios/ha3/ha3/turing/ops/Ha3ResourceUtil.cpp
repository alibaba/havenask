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
#include "ha3/turing/ops/Ha3ResourceUtil.h"

#include <memory>

#include "alog/Logger.h"
#include "suez/turing/common/CommonDefine.h"
#include "suez/turing/common/RuntimeState.h"
#include "suez/turing/expression/function/FunctionInterfaceCreator.h"
#include "suez/turing/expression/plugin/SorterManager.h"
#include "suez/turing/expression/util/TableInfo.h"
#include "tensorflow/core/platform/byte_order.h"

#include "ha3/common/AttributeItem.h"
#include "ha3/common/ConfigClause.h"
#include "ha3/common/Ha3MatchDocAllocator.h"
#include "ha3/common/Request.h"
#include "ha3/common/TimeoutTerminator.h"
#include "ha3/common/Tracer.h"
#include "ha3/monitor/SessionMetricsCollector.h"
#include "ha3/rank/ComparatorCreator.h"
#include "ha3/rank/RankProfileManager.h"
#include "ha3/search/DocCountLimits.h"
#include "ha3/search/OptimizerChainManager.h"
#include "ha3/search/SearchCommonResource.h"
#include "ha3/search/SortExpressionCreator.h"
#include "ha3/service/SearcherResource.h"
#include "ha3/service/ServiceDegrade.h"
#include "autil/Log.h"

namespace autil {
namespace mem_pool {
class Pool;
}  // namespace mem_pool
}  // namespace autil
namespace isearch {
namespace rank {
class RankProfile;
}  // namespace rank
}  // namespace isearch
namespace suez {
namespace turing {
class AttributeExpressionCreator;
class SuezCavaAllocator;
}  // namespace turing
}  // namespace suez

namespace isearch {
namespace turing {
AUTIL_LOG_SETUP(ha3, Ha3ResourceUtil);

using namespace isearch::rank;
using namespace isearch::monitor;
using namespace isearch::service;
using namespace isearch::common;
using namespace indexlib::index;
using namespace isearch::search;
using namespace indexlib::index;
using namespace indexlib::partition;
using namespace tensorflow;
using namespace suez::turing;
using namespace autil::mem_pool;

SearchCommonResourcePtr Ha3ResourceUtil::createSearchCommonResource(
        const Request *request,
        Pool *pool,
        SessionMetricsCollector *metricsCollector,
        TimeoutTerminator *timeoutTerminator,
        Tracer *tracer,
        SearcherResourcePtr &resourcePtr,
        SuezCavaAllocator *cavaAllocator,
        cava::CavaJitModulesWrapper *cavaJitModulesWrapper)
{
    TableInfoPtr tableInfoPtr = resourcePtr->getTableInfo();
    FunctionInterfaceCreatorPtr funcCreatorPtr = resourcePtr->getFuncCreator();
    OptimizerChainManagerPtr optimizerChainManagerPtr = resourcePtr->getOptimizerChainManager();
    SorterManagerPtr sorterManager = resourcePtr->getSorterManager();
    SearcherCachePtr searcherCachePtr = resourcePtr->getSearcherCache();
    SearchCommonResourcePtr commonResource(
            new SearchCommonResource(pool, tableInfoPtr, metricsCollector,
                    timeoutTerminator, tracer, funcCreatorPtr.get(),
                    resourcePtr->getCavaPluginManager().get(),
                    request, cavaAllocator, cavaJitModulesWrapper));
    return commonResource;
}

SearchPartitionResourcePtr Ha3ResourceUtil::createSearchPartitionResource(
        const Request *request,
        const IndexPartitionReaderWrapperPtr &indexPartReaderWrapperPtr,
        PartitionReaderSnapshot *partitionReaderSnapshot,
        SearchCommonResourcePtr &commonResource)
{
    SearchPartitionResourcePtr partitionResource(
            new SearchPartitionResource(*commonResource, request,
                    indexPartReaderWrapperPtr, partitionReaderSnapshot));
    return partitionResource;
}

SearchRuntimeResourcePtr Ha3ResourceUtil::createSearchRuntimeResource(
        const Request *request,
        const SearcherResourcePtr &searcherResource,
        const SearchCommonResourcePtr &commonResource,
        AttributeExpressionCreator *attributeExpressionCreator,
        uint32_t totalPartCount)
{
    SearchRuntimeResourcePtr runtimeResource;
    RankProfileManagerPtr rankProfileMgrPtr = searcherResource->getRankProfileManager();
    const RankProfile *rankProfile = NULL;
    if (!rankProfileMgrPtr->getRankProfile(request, rankProfile, commonResource->errorResult)) {
        AUTIL_LOG(WARN, "Get RankProfile Failed");
        return runtimeResource;
    }
    SortExpressionCreatorPtr exprCreator(new SortExpressionCreator(
                    attributeExpressionCreator, rankProfile,
                    commonResource->matchDocAllocator.get(),
                    commonResource->errorResult, commonResource->pool));

    runtimeResource.reset(new SearchRuntimeResource());
    runtimeResource->sortExpressionCreator = exprCreator;
    runtimeResource->comparatorCreator.reset(new ComparatorCreator(
                    commonResource->pool,
                    request->getConfigClause()->isOptimizeComparator()));

    runtimeResource->docCountLimits.init(request, rankProfile,
            searcherResource->getClusterConfig(),
            totalPartCount, commonResource->tracer);
    runtimeResource->rankProfile = rankProfile;
    return runtimeResource;
}


} // namespace turing
} // namespace isearch
