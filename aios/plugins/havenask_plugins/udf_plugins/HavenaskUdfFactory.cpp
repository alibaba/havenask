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
#include "havenask_plugins/udf_plugins/HavenaskUdfFactory.h"
#include "havenask_plugins/udf_plugins/vectorscore/VectorScoreFunction.h"
#include "havenask_plugins/udf_plugins/relevance/BM25ScoreFunction.h"
#include "havenask_plugins/udf_plugins/relevance/TfIdfScoreFunction.h"
#include "havenask_plugins/udf_plugins/sumof/SumOfFunction.h"
#include "suez/turing/expression/function/SyntaxExpressionFactory.h"

using namespace std;

USE_HAVENASK_UDF_NAMESPACE(bm25score);
USE_HAVENASK_UDF_NAMESPACE(tfidfscore);
USE_HAVENASK_UDF_NAMESPACE(vectorscore);
USE_HAVENASK_UDF_NAMESPACE(sumof);

BEGIN_HAVENASK_UDF_NAMESPACE(factory)

HavenaskUdfFactory::~HavenaskUdfFactory() {}

bool HavenaskUdfFactory::registeFunctions() {
    REGISTE_FUNCTION_CREATOR(VectorScoreFunctionCreatorImpl);
    REGISTE_FUNCTION_CREATOR(BM25ScoreFunctionCreatorImpl);
    REGISTE_FUNCTION_CREATOR(TfIdfScoreFunctionCreatorImpl);
    REGISTE_FUNCTION_CREATOR(SumOfFunctionCreatorImpl);
    return true;
}

END_HAVENASK_UDF_NAMESPACE(factory)
