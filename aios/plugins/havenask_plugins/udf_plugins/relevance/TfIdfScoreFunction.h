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
#include "havenask_plugins/udf_plugins/utils/common.h"
#include "turing_ops_util/variant/Tracer.h"
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

BEGIN_HAVENASK_UDF_NAMESPACE(tfidfscore);


class TfIdfScoreFunction : public suez::turing::FunctionInterfaceTyped<float>
{
public:
    TfIdfScoreFunction(const suez::turing::FunctionSubExprVec& exprs);
    ~TfIdfScoreFunction();

public:
    bool beginRequest(suez::turing::FunctionProvider* functionProvide) override;
    void endRequest() override;
    float evaluate(matchdoc::MatchDoc matchDoc) override;

private:    
    bool initMetas();


protected:
    std::string _scoreIndexName = "";
    suez::turing::FunctionSubExprVec _exprs;
    suez::turing::MatchInfoReader* _matchInfoReader;
    const matchdoc::Reference<suez::turing::MatchData>* _matchDataRef;
    suez::turing::FunctionProvider* _functionProvider;
    std::shared_ptr<suez::turing::MetaInfo> _metaInfo;
    int64_t _partTotalDocCount;
    relevance::FieldMetaHelper* _fieldMetaHelper;
    std::vector<size_t> _filteredTermMetaIdx;
    AUTIL_LOG_DECLARE();
};

DECLARE_FUNCTION_CREATOR(TfIdfScoreFunction, "tfidf_score", -1);

class TfIdfScoreFunctionCreatorImpl: public FUNCTION_CREATOR_CLASS_NAME(TfIdfScoreFunction) {
public:
    suez::turing::FunctionInterface *createFunction(
            const suez::turing::FunctionSubExprVec& funcSubExprVec);
private:
    AUTIL_LOG_DECLARE();
};

HAVENASK_UDF_TYPEDEF_PTR(TfIdfScoreFunction);

END_HAVENASK_UDF_NAMESPACE(tfidfscore);

