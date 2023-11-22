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
#include <vector>

#include "autil/Log.h"
#include "suez/turing/expression/function/FunctionCreator.h"
#include "suez/turing/expression/function/FunctionInterface.h"
#include "suez/turing/expression/provider/MatchInfoReader.h"
#include "suez/turing/expression/util/FieldMetaReaderWrapper.h"
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "suez/turing/expression/provider/FunctionProvider.h"
#include "suez/turing/expression/util/IndexInfoHelper.h"
#include "havenask_plugins/udf_plugins/utils/common.h"
#include "havenask_plugins/udf_plugins/relevance/FieldMetaHelper.h"

namespace matchdoc {
class MatchDoc;
class ReferenceBase;
}  // namespace matchdoc
namespace suez {
namespace turing {
class FunctionProvider;
}  // namespace turing
}  // namespace suez

BEGIN_HAVENASK_UDF_NAMESPACE(bm25score);

// usage: bm25score('index_name', total_doc_count, average_doc_length)
class BM25ScoreFunction : public suez::turing::FunctionInterfaceTyped<float>
{
public:
    BM25ScoreFunction(const suez::turing::FunctionSubExprVec& exprs);
    ~BM25ScoreFunction();

public:
    bool beginRequest(suez::turing::FunctionProvider* functionProvider) override;
    void endRequest() override;
    float evaluate(matchdoc::MatchDoc matchDoc) override;

private:
    bool getIndexLengthOfDoc(int32_t docid, std::string indexName, int64_t& docLength, std::map<std::string, int64_t>& indexNameToIndexLength);
    bool initMetas();

protected:
    suez::turing::FunctionSubExprVec _exprs;
    std::string _scoreIndexName = "";
    int64_t _partTotalDocCount;
    suez::turing::MatchInfoReader* _matchInfoReader;
     const matchdoc::Reference<suez::turing::MatchData>* _matchDataRef;
    suez::turing::MetaInfo* _metaInfo;
    suez::turing::FunctionProvider* _functionProvider;
    relevance::FieldMetaHelper* _fieldMetaHelper;

    std::map<size_t, double> _metaInfoIdxToAvgLength;
    std::vector<size_t> _filteredTermMetaIdx;

private:
    AUTIL_LOG_DECLARE();
};

DECLARE_FUNCTION_CREATOR(BM25ScoreFunction, "bm25_score", -1);

class BM25ScoreFunctionCreatorImpl: public FUNCTION_CREATOR_CLASS_NAME(BM25ScoreFunction) {
public:
    suez::turing::FunctionInterface *createFunction(
            const suez::turing::FunctionSubExprVec& funcSubExprVec);
private:
    AUTIL_LOG_DECLARE();
};



HAVENASK_UDF_TYPEDEF_PTR(BM25ScoreFunction);

END_HAVENASK_UDF_NAMESPACE(bm25score);

