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

#include <assert.h>
#include <set>
#include <string>

#include "autil/CommonMacros.h"
#include "autil/Log.h"
#include "autil/TimeUtility.h"
#include "matchdoc/MatchDoc.h"
#include "suez/turing/expression/function/FunctionCreator.h"
#include "suez/turing/expression/function/FunctionInterface.h"

namespace suez {
namespace turing {
class FunctionProvider;

class TimeFuncInterface : public FunctionInterfaceTyped<int64_t> {
public:
    TimeFuncInterface() {}
    ~TimeFuncInterface() {}

public:
    bool beginRequest(FunctionProvider *provider) override { return true; }
    int64_t evaluate(matchdoc::MatchDoc matchDoc) override {
        if (currentTimeStamp == 0) {
            currentTimeStamp = autil::TimeUtility::currentTimeInSeconds();
        }
        return currentTimeStamp;
    }

private:
    int64_t currentTimeStamp = 0;

private:
    AUTIL_LOG_DECLARE();
};
////////////////////////////////////////////////////////////

DECLARE_FUNCTION_CREATOR(TimeFuncInterface, "currentTimeInSeconds", 0);

class TimeFuncInterfaceCreatorImpl : public TimeFuncInterfaceCreator {
public:
    FunctionInterface *createFunction(const FunctionSubExprVec &funcSubExprVec) override {
        return new TimeFuncInterface();
    }
};

} // namespace turing
} // namespace suez
