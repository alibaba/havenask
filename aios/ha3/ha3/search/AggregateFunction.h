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

#include <stddef.h>
#include <stdint.h>
#include <string>

#include "matchdoc/MatchDoc.h"
#include "sys/types.h"

#include "ha3/isearch.h"
#include "autil/Log.h" // IWYU pragma: keep

namespace autil {
namespace mem_pool {
class Pool;
}  // namespace mem_pool
}  // namespace autil
namespace matchdoc {
class MatchDocAllocator;
class ReferenceBase;
template <typename T> class Reference;
}  // namespace matchdoc
namespace suez {
namespace turing {
class AttributeExpression;
}  // namespace turing
}  // namespace suez

namespace isearch {
namespace search {

class AggregateFunction
{
public:
    AggregateFunction(const std::string &functionName,
                      const std::string &parameter,
                      VariableType resultType);
    virtual ~AggregateFunction();

public:
    virtual void calculate(matchdoc::MatchDoc matchDoc, matchdoc::MatchDoc aggMatchDoc) = 0;
    virtual void declareResultReference(matchdoc::MatchDocAllocator *aggAllocator) = 0;
    virtual matchdoc::ReferenceBase *getReference(uint id = 0) const = 0;
    virtual void estimateResult(double factor, matchdoc::MatchDoc aggMatchDoc) {
        return;
    }
    virtual void beginSample(matchdoc::MatchDoc aggMatchDoc) {
        return;
    }
    virtual void endLayer(uint32_t sampleStep, double factor,
                          matchdoc::MatchDoc aggMatchDoc)
    {
        return;
    }

    template<typename T>
    const matchdoc::Reference<T> *getReferenceTyped() const {
        return dynamic_cast<const matchdoc::Reference<T> *>(getReference());
    }

    virtual void initFunction(matchdoc::MatchDoc aggMatchDoc, autil::mem_pool::Pool* pool = NULL) = 0;
    virtual std::string getStrValue(matchdoc::MatchDoc aggMatchDoc) const = 0;
    virtual suez::turing::AttributeExpression *getExpr() = 0;

    virtual bool canCodegen() {
        return false;
    }

    const std::string& getFunctionName() const {
        return _functionName;
    }
    const std::string& getParameter() const {
        return _parameter;
    }

    VariableType getResultType() const{
        return _resultType;
    }

    const std::string toString() const {
        return _functionName + "(" + _parameter + ")";
    }

protected:
    const std::string _functionName;
    const std::string _parameter;
    VariableType _resultType;
private:
    AUTIL_LOG_DECLARE();
};

} // namespace search
} // namespace isearch

