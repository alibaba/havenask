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
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/common/MultiErrorResult.h"
#include "ha3/common/QueryTokenizer.h"
#include "ha3/common/Request.h"
#include "ha3/common/Result.h"
#include "ha3/config/ClusterConfigInfo.h"
#include "ha3/isearch.h"
#include "ha3/qrs/QrsProcessor.h"
#include "ha3/qrs/RequestValidator.h"
#include "suez/turing/expression/util/TableInfo.h"

namespace isearch {
namespace common {
class ConfigClause;
class LevelClause;
class Query;
}  // namespace common
}  // namespace isearch
namespace suez {
namespace turing {
class AttributeInfos;
class IndexInfo;
class IndexInfos;
}  // namespace turing
}  // namespace suez

namespace isearch {
namespace qrs {

class RequestValidateProcessor : public QrsProcessor
{
public:
    RequestValidateProcessor(
            const suez::turing::ClusterTableInfoMapPtr &clusterTableInfoMapPtr,
            const config::ClusterConfigMapPtr &clusterConfigMapPtr,
            RequestValidatorPtr &requestValidatorPtr,
            common::QueryTokenizer &queryTokenizer);
    virtual ~RequestValidateProcessor();
public:
    virtual void process(common::RequestPtr &requestPtr,
                         common::ResultPtr &resultPtr);
    virtual QrsProcessorPtr clone();
public:
    std::string getName() const {
        return "RequestValidateProcessor";
    }
public:
    static void fillPhaseOneInfoMask(common::ConfigClause *configClause,
            const suez::turing::IndexInfo *summaryPrimaryKeyIndex);
private:
    bool validateRequest(common::RequestPtr &requestPtr,
                         common::ResultPtr &resultPtr);
    bool validateMultiCluster(std::vector<std::string> &clusterNameVec,
                              const common::LevelClause *levelClause,
                              common::MultiErrorResultPtr &errorResultPtr);
    bool validatePbMatchDocFormat(common::ConfigClause *configClause,
                                  common::MultiErrorResultPtr &errorResultPtr);
    bool validateFetchSummaryCluster(common::ConfigClause *configClause,
            common::MultiErrorResultPtr &errorResultPtr);

    bool getTableInfo(const std::string& clusterName,
                      common::MultiErrorResultPtr &errorResultPtr,
                      suez::turing::TableInfoPtr &tableInfoPtr);
    bool schemaCompatible(const suez::turing::TableInfoPtr &tableInfoPtr1,
                          const suez::turing::TableInfoPtr &tableInfoPtr2);
    bool indexInfoCompatible(const suez::turing::IndexInfos* indexInfo1,
                             const suez::turing::IndexInfos* indexInfo2);
    bool attributeInfoCompatible(const suez::turing::AttributeInfos* attrInfo1,
                                 const suez::turing::AttributeInfos* attrInfo2);
    void setClusterTableInfoMapPtr(const suez::turing::ClusterTableInfoMapPtr &tableInfoMap);
    void setClusterConfigInfoMap(const config::ClusterConfigMapPtr &clusterConfigMapPtr);
    bool getClusterConfigInfo(const std::string &clusterName,
                              config::ClusterConfigInfo& clusterConfig) const;
    bool tokenizeAndCleanStopWord(common::Query *rootQuery, QueryOperator defaultOP,
                                  const suez::turing::IndexInfos *indexInfos,
                                  const common::MultiErrorResultPtr &errorResultPtr,
                                  common::Query **ppCleanQuery);
    //getAuxTableInfo();
    bool getAuxTableInfo(
            const std::string &clusterName,
            const common::MultiErrorResultPtr &errorResultPtr,
            suez::turing::TableInfoPtr &auxTableInfoPtr);
    bool tokenizeQuery(const common::RequestPtr& requestPtr,
                       const common::MultiErrorResultPtr &errorResultPtr,
                       const suez::turing::TableInfoPtr &mainTableInfoPtr,
                       const std::string &clusterName);
    static std::string getBizName(const std::string &name);
private:
    config::ClusterConfigMapPtr _clusterConfigMapPtr;
    suez::turing::ClusterTableInfoMapPtr _clusterTableInfoMapPtr;
    common::QueryTokenizer _queryTokenizer;
    RequestValidator _requestValidator;

    friend class RequestValidateProcessorTest;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<RequestValidateProcessor> RequestValidateProcessorPtr;

} // namespace qrs
} // namespace isearch
