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
#include "indexlib/document/query/pattern_query_evaluator.h"

using namespace std;

namespace indexlib { namespace document {
IE_LOG_SETUP(document, PatternQueryEvaluator);

void PatternQueryEvaluator::Init(const string& fieldName, const string& fieldPattern)
{
    mFieldName = fieldName;
    mFieldPattern = fieldPattern;
    mRegExpr.reset(new util::RegularExpression);
    if (!mRegExpr->Init(mFieldPattern)) {
        AUTIL_LOG(ERROR, "invalid field pattern [%s] for field [%s] in PatternQuery.", mFieldPattern.c_str(),
                  mFieldName.c_str());
        mRegExpr.reset();
    }
}

void PatternQueryEvaluator::Init(const FunctionExecutorPtr& function, const string& fieldPattern)
{
    assert(function);
    mFunc = function;
    mFieldName = mFunc->GetFunctionKey();
    mFieldPattern = fieldPattern;
    mRegExpr.reset(new util::RegularExpression);
    if (!mRegExpr->Init(mFieldPattern)) {
        AUTIL_LOG(ERROR, "invalid field pattern [%s] for field [%s] in PatternQuery.", mFieldPattern.c_str(),
                  mFieldName.c_str());
        mRegExpr.reset();
    }
}

EvaluatorState PatternQueryEvaluator::DoEvaluate(EvaluatorStateHandler& handler, const RawDocumentPtr& rawDoc,
                                                 EvaluateParam param)
{
    if (!mRegExpr) {
        return ES_FALSE;
    }

    if (!rawDoc) {
        return ES_PENDING;
    }

    if (mFunc) {
        mFunc->Execute(rawDoc);
    }
    autil::StringView fn(mFieldName);
    if (!rawDoc->exist(fn)) {
        return param.pendingForFieldNotExist ? ES_PENDING : ES_FALSE;
    }
    autil::StringView fv = rawDoc->getField(fn);
    return mRegExpr->Match(fv) ? ES_TRUE : ES_FALSE;
}
}} // namespace indexlib::document
