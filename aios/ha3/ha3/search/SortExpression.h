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

#include <stdint.h>
#include <memory>
#include <string>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/common/Ha3MatchDocAllocator.h"
#include "ha3/isearch.h"
#include "matchdoc/MatchDoc.h"
#include "suez/turing/expression/framework/AttributeExpression.h"

namespace matchdoc {
class ReferenceBase;
}  // namespace matchdoc

namespace isearch {
namespace search {

class SortExpression {
public:
    SortExpression(suez::turing::AttributeExpression *attributeExpression, bool sortFlag = false);
    ~SortExpression();
private:
    SortExpression(const SortExpression &);
    SortExpression& operator=(const SortExpression &);
public:
    suez::turing::AttributeExpression* getAttributeExpression() {
        return _attributeExpression; 
    }
    bool allocate(common::Ha3MatchDocAllocator *slabAllocator) {
        return _attributeExpression->allocate(slabAllocator);
    }
    bool evaluate(matchdoc::MatchDoc matchDoc) {
        return _attributeExpression->evaluate(matchDoc);
    }
    bool batchEvaluate(matchdoc::MatchDoc *matchDocs, uint32_t matchDocCount) {
        return _attributeExpression->batchEvaluate(matchDocs, matchDocCount);
    }
    matchdoc::ReferenceBase* getReferenceBase() const {
        return _attributeExpression->getReferenceBase();
    }
    void setEvaluated() {
        return _attributeExpression->setEvaluated();
    }
    bool isEvaluated() const {
        return _attributeExpression->isEvaluated();
    }
    VariableType getType() const {
        return _attributeExpression->getType();
    }
    void setSortFlag(bool flag) {
        _sortFlag = flag;
    }
    bool getSortFlag() const {
        return _sortFlag;
    }
    const std::string& getOriginalString() const { 
        return _attributeExpression->getOriginalString(); 
    }
    void setOriginalString(const std::string& originalString) { 
        _attributeExpression->setOriginalString(originalString);
    }
    bool isMultiValue() const {
        return _attributeExpression->isMultiValue();
    }
private:
    bool _sortFlag;
    suez::turing::AttributeExpression *_attributeExpression;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::vector<SortExpression*> SortExpressionVector;
typedef std::shared_ptr<SortExpression> SortExpressionPtr;

} // namespace search
} // namespace isearch

