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

#include <map>
#include <set>
#include <string>

#include "autil/legacy/jsonizable.h"
#include "build_service/analyzer/AnalyzerFactory.h"
#include "suez/turing/expression/util/TableInfo.h"
#include "indexlib/indexlib.h"

#include "ha3/common/Hit.h"
#include "ha3/common/Request.h"
#include "ha3/common/Result.h"
#include "ha3/config/ResourceReader.h"
#include "ha3/isearch.h"
#include "ha3/qrs/QrsProcessor.h"
#include "autil/Log.h" // IWYU pragma: keep

namespace isearch {
namespace common {
class Query;
}  // namespace common
namespace qrs {
struct CheckResultInfo;
}  // namespace qrs
}  // namespace isearch

namespace isearch {
namespace qrs {
class CheckResult;

class MatchInfoProcessor : public QrsProcessor
{
public:
    MatchInfoProcessor(const suez::turing::ClusterTableInfoMapPtr &clusterTableInfoMapPtr)
        : _clusterTableInfoMapPtr(clusterTableInfoMapPtr)
    {
    }

    virtual ~MatchInfoProcessor(){}
public:
    
    std::string getName() const override {
        return DEFAULT_DEBUG_PROCESSOR;
    }
    bool init(const KeyValueMap &keyValues, 
              config::ResourceReader *resourceReader) override;
    void process(common::RequestPtr &requestPtr,
                 common::ResultPtr &resultPtr) override;
    void fillSummary(const common::RequestPtr &requestPtr,
                     const common::ResultPtr &resultPtr) override;
    QrsProcessorPtr clone() override;
    
    void setAnalyzerFactory(const build_service::analyzer::AnalyzerFactoryPtr &analyzerFactoryPtr) {
        _analyzerFactoryPtr = analyzerFactoryPtr;
    }

private:
    bool rewriteQuery(common::RequestPtr &requestPtr);
    void analyzeMatchInfo(const common::RequestPtr &requestPtr, 
                          const common::ResultPtr &resultPtr);
    bool findIndex(const suez::turing::TableInfoPtr& tableInfoPtr,
                   const std::string &indexName);
    void reportMatchResult(const CheckResult& checkResult);
    void traceQuery(const std::string& prefix, const common::Query *query);

    void getSummaryInfo(const suez::turing::TableInfoPtr& tableInfoPtr, 
                        std::map<std::string, std::string>& summaryInfo);
    std::map<std::string, std::string> getSummaryFields(const common::HitPtr& hitPtr,
            const std::map<std::string, std::string>& summaryInfo);
    void tokenSummary(std::map<std::string, std::string>& summaryMap,
                      const std::map<std::string, std::string>& summaryInfo);

    std::string getAnalyzerName(const std::string& fieldName, 
                                const std::map<std::string, std::string>& summaryInfo);
    void getAttributes(std::map<std::string, std::string>& attributeMap,
                       const common::HitPtr& hitPtr);
    void checkPhraseError(const std::map<std::string, std::string>& phraseMap, 
                          const std::map<std::string, std::string>& summaryMap, 
                          CheckResultInfo& resInfo);
    void removeSummaryTag(std::string& titleStr);
    void replaceAll(std::string& str, const std::string& from, 
                    const std::string& to); 
    suez::turing::TableInfoPtr getTableInfo(const std::string& clusterName) const;
    inline bool indexNeedTokenize(InvertedIndexType indexType) {
        return (it_text == indexType) || (it_pack == indexType)
            || (it_expack == indexType);
    }
private:
    suez::turing::ClusterTableInfoMapPtr _clusterTableInfoMapPtr;
    build_service::analyzer::AnalyzerFactoryPtr _analyzerFactoryPtr; //for tokenize summary info.
    std::set<std::string> _queryIndexs;
    std::map<std::string, std::string> _summaryFields;
private:
    friend class MatchInfoProcessorTest;
    AUTIL_LOG_DECLARE();
};

class CheckQueryResult : public autil::legacy::Jsonizable{
public:
    std::string _queryStr;
    std::string _errorSubQueryStr;
    std::string _errorType;
    std::string _errorWord;
    bool _isTokenizePart;
    std::map<std::string, std::string> _summaryMap;
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);  
};

class CheckFilterResult : public autil::legacy::Jsonizable{
public:
    std::map<std::string, std::string> _filterValMap;
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);  
};

class CheckResult : public autil::legacy::Jsonizable{
public:
    CheckQueryResult _checkQueryResult;
    CheckFilterResult _checkFilterResult;
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);  
};

} // namespace qrs
} // namespace isearch
