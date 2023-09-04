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
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "autil/Log.h"
#include "autil/legacy/RapidJsonCommon.h"
#include "autil/plugin/BaseInterface.h"
#include "ha3/isearch.h"
#include "ha3/turing/common/ModelConfig.h"

namespace build_service {
namespace analyzer {
class AnalyzerFactory;
} // namespace analyzer
} // namespace build_service
namespace isearch {
namespace common {
class Query;
} // namespace common
} // namespace isearch
namespace suez {
namespace turing {
class IndexInfos;
} // namespace turing
} // namespace suez

namespace isearch {

namespace common {
class QueryInfo;
} // namespace common
} // namespace isearch

namespace sql {

class IndexInfo;

class UdfToQueryParam {
public:
    const build_service::analyzer::AnalyzerFactory *analyzerFactory = nullptr;
    const std::map<std::string, IndexInfo> *indexInfo = nullptr;
    const suez::turing::IndexInfos *indexInfos = nullptr;
    const isearch::common::QueryInfo *queryInfo = nullptr;
    const isearch::turing::ModelConfigMap *modelConfigMap = nullptr;
    const std::unordered_set<std::string> *forbidIndexs = nullptr;
};

class UdfToQuery : public autil::BaseInterface {
public:
    UdfToQuery() {}
    virtual ~UdfToQuery() {}

public:
    virtual isearch::common::Query *toQuery(const autil::SimpleValue &condition,
                                            const UdfToQueryParam &condParam)
        = 0;
    virtual bool onlyQuery() {
        return true;
    }

public:
    static void parseKvPairInfo(const std::string &kvStr,
                                std::unordered_map<std::string, std::string> &indexAnalyzerMap,
                                std::string &globalAnalyzer,
                                std::string &defaultOpStr,
                                std::unordered_set<std::string> &noTokenIndexes,
                                bool &tokenizeQuery,
                                bool &removeStopWords);
    static isearch::common::Query *
    postProcessQuery(bool tokenQuery,
                     bool removeStopWords,
                     QueryOperator op,
                     const std::string &queryStr,
                     const std::string &globalAnalyzer,
                     const std::unordered_set<std::string> &noTokenIndexs,
                     const std::unordered_map<std::string, std::string> &indexAnalyzerMap,
                     const suez::turing::IndexInfos *indexInfos,
                     const build_service::analyzer::AnalyzerFactory *analyzerFactory,
                     isearch::common::Query *query);
    static isearch::common::Query *reserveFirstQuery(std::vector<isearch::common::Query *> &querys);

private:
    static isearch::common::Query *
    tokenizeQuery(isearch::common::Query *query,
                  QueryOperator qp,
                  const std::unordered_set<std::string> &noTokenIndexes,
                  const std::string &globalAnalyzer,
                  const std::unordered_map<std::string, std::string> &indexAnalyzerMap,
                  bool removeStopWords,
                  const suez::turing::IndexInfos *indexInfos,
                  const build_service::analyzer::AnalyzerFactory *analyzerFactory);
    static isearch::common::Query *cleanStopWords(isearch::common::Query *query);
    static bool validateQuery(isearch::common::Query *query,
                              const suez::turing::IndexInfos *indexInfos);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace sql
