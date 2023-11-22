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

#include "havenask_plugins/udf_plugins/utils/common.h"

namespace matchdoc {
class MatchDoc;
class ReferenceBase;
}  // namespace matchdoc
namespace suez {
namespace turing {
class FunctionProvider;
}  // namespace turing
}  // namespace suez

BEGIN_HAVENASK_UDF_NAMESPACE(vectorscore);

class VectorScoreFunction : public suez::turing::FunctionInterfaceTyped<double>
{
public:
    VectorScoreFunction(const suez::turing::FunctionSubExprVec& exprs);
    ~VectorScoreFunction();

public:
    bool beginRequest(suez::turing::FunctionProvider* functionProvider) override;
    void endRequest() override;
    double evaluate(matchdoc::MatchDoc matchDoc) override;

protected:
    suez::turing::FunctionSubExprVec _exprs;
    std::string _vectorIndexName;
    suez::turing::MatchInfoReader* _matchInfoReader;
    matchdoc::Reference<suez::turing::MatchValues>* _matchValuesRef;
    double _defaultScore = 1000.0;
    AUTIL_LOG_DECLARE();
};

DECLARE_FUNCTION_CREATOR(VectorScoreFunction, "vector_score", -1);

class VectorScoreFunctionCreatorImpl: public FUNCTION_CREATOR_CLASS_NAME(VectorScoreFunction) {
public:
    suez::turing::FunctionInterface *createFunction(
            const suez::turing::FunctionSubExprVec& funcSubExprVec);
private:
    AUTIL_LOG_DECLARE();
};

HAVENASK_UDF_TYPEDEF_PTR(VectorScoreFunction);

END_HAVENASK_UDF_NAMESPACE(vectorscore);

