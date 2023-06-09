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
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "autil/Log.h"
#include "autil/legacy/RapidJsonCommon.h"
#include "ha3/isearch.h"
#include "autil/plugin/BaseInterface.h"

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
namespace sql {

class Ha3ScanConditionVisitorParam;

class UdfToQuery : public autil::BaseInterface {
public:
    UdfToQuery() {}
    virtual ~UdfToQuery() {}

public:
    virtual isearch::common::Query *toQuery(const autil::SimpleValue &condition,
                                            const Ha3ScanConditionVisitorParam &condParam)
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
    static common::Query *
    postProcessQuery(bool tokenQuery,
                     bool removeStopWords,
                     QueryOperator op,
                     const std::string &queryStr,
                     const std::string &globalAnalyzer,
                     const std::unordered_set<std::string> &noTokenIndexs,
                     const std::unordered_map<std::string, std::string> &indexAnalyzerMap,
                     const suez::turing::IndexInfos *indexInfos,
                     const build_service::analyzer::AnalyzerFactory *analyzerFactory,
                     common::Query *query);
    static common::Query *reserveFirstQuery(std::vector<common::Query *> &querys);

private:
    static common::Query *
    tokenizeQuery(common::Query *query,
                  QueryOperator qp,
                  const std::unordered_set<std::string> &noTokenIndexes,
                  const std::string &globalAnalyzer,
                  const std::unordered_map<std::string, std::string> &indexAnalyzerMap,
                  bool removeStopWords,
                  const suez::turing::IndexInfos *indexInfos,
                  const build_service::analyzer::AnalyzerFactory *analyzerFactory);
    static common::Query *cleanStopWords(common::Query *query);
    static bool validateQuery(common::Query *query, const suez::turing::IndexInfos *indexInfos);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace sql
} // namespace isearch
