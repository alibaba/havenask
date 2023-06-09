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
#ifndef ISEARCH_EXPRESSION_CONTEXTATTRIBUTEEXPRESSION_H
#define ISEARCH_EXPRESSION_CONTEXTATTRIBUTEEXPRESSION_H

#include "expression/common.h"
#include "expression/framework/AttributeExpressionTyped.h"
#include "expression/framework/TypeTraits.h"
#include <algorithm>

namespace expression {

template <typename T>
class ContextAttributeExpression : public AttributeExpressionTyped<T>
{
public:
    ContextAttributeExpression(const std::string &name)
        : AttributeExpressionTyped<T>(ET_CONTEXT,
                AttrValueType2ExprValueType<T>::evt,
                AttrValueType2ExprValueType<T>::isMulti,
                name)
        , _docCount(0)
        , _docIds(NULL)
        , _values(NULL)
        , _cursor(-1)
    {}
    ContextAttributeExpression(const std::string &name,
                               uint32_t docCount,
                               docid_t *docIds,
                               T *values)
        : AttributeExpressionTyped<T>(ET_CONTEXT,
                AttrValueType2ExprValueType<T>::evt,
                AttrValueType2ExprValueType<T>::isMulti,
                name)
        , _docCount(docCount)
        , _docIds(docIds)
        , _values(values)
        , _cursor(-1)
    {}
    ~ContextAttributeExpression() {
    }
private:
    ContextAttributeExpression(const ContextAttributeExpression &);
    ContextAttributeExpression& operator=(const ContextAttributeExpression &);
public:
    /* override */ void evaluate(const matchdoc::MatchDoc &matchDoc) {
        docid_t docId = matchDoc.getDocId();
        int32_t next = _cursor + 1;
        if (next >= (int32_t)_docCount || _docIds[next] != docId) {
            int32_t *r = std::lower_bound(_docIds, _docIds + _docCount, docId);
            int32_t pos = r - _docIds;
            if (pos >= (int32_t)_docCount || _docIds[pos] != docId) {
                this->storeValue(matchDoc, T());
                return;
            }
            next = pos;
        }
        this->storeValue(matchDoc, _values[next]);
        _cursor = next;
    }
    /* override */ void batchEvaluate(matchdoc::MatchDoc *matchDocs, uint32_t docCount) {
        int32_t next = _cursor + 1;
        for (uint32_t i = 0; i < docCount; i++) {
            docid_t docId = matchDocs[i].getDocId();
            if (next >= (int32_t)_docCount || _docIds[next] != docId) {
                docid_t *r = std::lower_bound(_docIds, _docIds + _docCount, docId);
                int32_t pos = r - _docIds;
                if (pos >= (int32_t)_docCount || _docIds[pos] != docId) {
                    this->storeValue(matchDocs[i], T());// set default value
                    continue;
                }
                next = pos; // found
            }
            this->storeValue(matchDocs[i], _values[next]);
            next++;
        }
        _cursor = next;
    }
    /* override */ void reset() {
        AttributeExpressionTyped<T>::reset();        
        _docCount = 0;
        _docIds = NULL;
        _values = NULL;
        _cursor = -1;
    }
    /* override */ bool check() const {
        return _docIds != NULL && _values != NULL && _docCount > 0;
    }
    void setValue(uint32_t docCount, docid_t *docIds, T *values) {
        _docCount = docCount;
        _docIds = docIds;
        _values = values;
    }
private:
    uint32_t _docCount;
    docid_t *_docIds;
    T *_values;
    int32_t _cursor;
private:
    AUTIL_LOG_DECLARE();
};

}

#endif //ISEARCH_EXPRESSION_CONTEXTATTRIBUTEEXPRESSION_H
