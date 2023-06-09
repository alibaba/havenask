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
#include <memory>
#include <string>

#include "autil/CompressionUtil.h"
#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/common/Request.h"
#include "ha3/common/Result.h"
#include "ha3/config/ClusterConfigInfo.h"
#include "ha3/isearch.h"
#include "ha3/qrs/QrsProcessor.h"
#include "ha3/service/HitSummarySchemaCache.h"
#include "suez/turing/expression/util/TableInfo.h"

namespace isearch {
namespace common {
class ConfigClause;
class Hits;
class QueryClause;
}  // namespace common
namespace config {
struct SearchTaskQueueRule;
}  // namespace config
namespace service {
class QrsSearcherProcessDelegation;
}  // namespace service
}  // namespace isearch

namespace isearch {
namespace qrs {
class QrsSearchProcessor : public QrsProcessor
{
public:
    QrsSearchProcessor(
            int32_t connectionTimeout,
            const suez::turing::ClusterTableInfoMapPtr &clusterTableInfoMapPtr,
            const config::ClusterConfigMapPtr &clusterConfigMapPtr,
            const suez::turing::ClusterSorterManagersPtr &clusterSorterManagersPtr,
            autil::CompressType requestCompressType,
            const config::SearchTaskQueueRule &searchTaskqueueRule);
    ~QrsSearchProcessor();
private:
    QrsSearchProcessor(const QrsSearchProcessor& processor);
public:
    virtual void process(common::RequestPtr &requestPtr,
                         common::ResultPtr &resultPtr);
    virtual QrsProcessorPtr clone();
    virtual void fillSummary(const common::RequestPtr &requestPtr,
                             const common::ResultPtr &resultPtr);
    std::string getName() const { return "QrsSearchProcessor"; }
    // for test
    const service::QrsSearcherProcessDelegation* getSearcherDelegation() const {
        return _qrsSearcherProcessDelegation;
    }
private:
    void initSearcherDelegation();
    void initQrsSearchProcesser();
    common::ResultPtr convertFetchSummaryClauseToHits(
            const common::RequestPtr &requestPtr);
    uint32_t calculateHashId(const std::string &clusterName,
                             const std::string &rawPk);
    primarykey_t calculatePrimaryKey(const std::string &clusterName,
            const std::string &rawPk);
    static void fillSearcherCacheKey(common::RequestPtr &requestPtr);
    void convertSummaryCluster(common::Hits *hits, common::ConfigClause *configClause);
    void fillPositionForHits(const common::ResultPtr &result);
    void rewriteMetricSrc(common::ConfigClause *configClause);
private:
    // for test
    void setSearcherDelegation(service::QrsSearcherProcessDelegation* delegation);
    void flatten(common::QueryClause *queryClause);
private:
    suez::turing::ClusterTableInfoMapPtr _clusterTableInfoMapPtr;
    config::ClusterConfigMapPtr _clusterConfigMapPtr;
    service::HitSummarySchemaCachePtr _hitSummarySchemaCache;
    service::QrsSearcherProcessDelegation *_qrsSearcherProcessDelegation;
    suez::turing::ClusterSorterManagersPtr _clusterSorterManagersPtr;
private:
    friend class QrsSearchProcessorTest;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<QrsSearchProcessor> QrsSearchProcessorPtr;

} // namespace qrs
} // namespace isearch
