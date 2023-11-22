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

#include "indexlib/common_define.h"
#include "indexlib/document/query/query_function.h"
#include "indexlib/document/raw_document.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/KeyValueMap.h"

namespace indexlib { namespace document {

class FunctionExecutor
{
public:
    FunctionExecutor() {}
    virtual ~FunctionExecutor() {}

public:
    bool Init(const QueryFunction& queryFunction, const util::KeyValueMap& kvMap)
    {
        mFunction = queryFunction;
        mFunctionKey = mFunction.GetFunctionKey();
        return DoInit(kvMap);
    }

    void Execute(const RawDocumentPtr& rawDoc)
    {
        if (rawDoc->exist(mFunctionKey)) {
            // already execute
            return;
        }

        bool hasError = false;
        std::string value = ExecuteFunction(rawDoc, hasError);
        if (!hasError) {
            rawDoc->setField(mFunctionKey, value);
        }
    }

    const std::string& GetFunctionKey() const { return mFunctionKey; }

    virtual bool DoInit(const util::KeyValueMap& kvMap) = 0;
    virtual std::string ExecuteFunction(const RawDocumentPtr& rawDoc, bool& hasError) = 0;

protected:
    QueryFunction mFunction;
    std::string mFunctionKey;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(FunctionExecutor);
}} // namespace indexlib::document
