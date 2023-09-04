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
#include <stdint.h>
#include <string>

#include "autil/mem_pool/PoolBase.h"
#include "indexlib/index/normal/attribute/accessor/attribute_iterator_typed.h"
#include "indexlib/indexlib.h"
#include "indexlib/misc/common.h"
#include "matchdoc/MatchDoc.h"
#include "suez/turing/expression/framework/AttributeExpression.h"
#include "suez/turing/expression/framework/DocIdAccessor.h"

namespace suez {
namespace turing {

template <typename T,
          typename DocIdAccessor = DefaultDocIdAccessor,
          typename AttrIterator = indexlib::index::AttributeIteratorTyped<T>>
class AtomicAttributeExpression : public AttributeExpressionTyped<T> {
public:
    typedef AttrIterator Iterator;

public:
    AtomicAttributeExpression(const std::string &attriName,
                              Iterator *attributeIterator,
                              const DocIdAccessor &docIdAccessor = DocIdAccessor())
        : _attributeName(attriName), _iterator(attributeIterator), _docIdAccessor(docIdAccessor) {}

    ~AtomicAttributeExpression() { POOL_DELETE_CLASS(_iterator); }

public:
    bool evaluate(matchdoc::MatchDoc matchDoc) override;
    T evaluateAndReturn(matchdoc::MatchDoc matchDoc) override;
    bool batchEvaluate(matchdoc::MatchDoc *matchDocs, uint32_t matchDocCount) override;
    bool operator==(const AttributeExpression *checkExpr) const override;
    ExpressionType getExpressionType() const override { return ET_ATOMIC; }

public:
    // for test
    Iterator *getAttributeIterator() const { return _iterator; }

private:
    const std::string _attributeName;
    Iterator *_iterator;
    DocIdAccessor _docIdAccessor;
};

template <typename T, typename DocIdAccessor, typename AttrIterator>
inline bool AtomicAttributeExpression<T, DocIdAccessor, AttrIterator>::evaluate(matchdoc::MatchDoc matchDoc) {
    T value = T();
    if (!this->tryFetchValue(matchDoc, value)) { // avoid virtual call
        docid_t docId = _docIdAccessor.getDocId(matchDoc);
        (void)_iterator->Seek(docId, value);
        this->storeValue(matchDoc, value);
    }
    this->setValue(value);
    return true;
}

template <typename T, typename DocIdAccessor, typename AttrIterator>
inline T AtomicAttributeExpression<T, DocIdAccessor, AttrIterator>::evaluateAndReturn(matchdoc::MatchDoc matchDoc) {
    assert(matchdoc::INVALID_MATCHDOC != matchDoc);
    T value = T();
    if (!this->tryFetchValue(matchDoc, value)) {
        docid_t docId = _docIdAccessor.getDocId(matchDoc);
        (void)_iterator->Seek(docId, value);
        this->storeValue(matchDoc, value);
    }
    return value;
}

template <typename T, typename DocIdAccessor, typename AttrIterator>
inline bool AtomicAttributeExpression<T, DocIdAccessor, AttrIterator>::batchEvaluate(matchdoc::MatchDoc *matchDocs,
                                                                                     uint32_t matchDocCount) {
    if (this->isEvaluated()) {
        return true;
    }
    for (uint32_t i = 0; i < matchDocCount; ++i) {
        matchdoc::MatchDoc matchDoc = matchDocs[i];
        docid_t docId = _docIdAccessor.getDocId(matchDoc);
        T value = T();
        _iterator->Seek(docId, value);
        this->storeValue(matchDoc, value);
    }
    return true;
}

template <typename T, typename DocIdAccessor, typename AttrIterator>
bool AtomicAttributeExpression<T, DocIdAccessor, AttrIterator>::operator==(const AttributeExpression *checkExpr) const {
    assert(checkExpr);
    const AtomicAttributeExpression<T, DocIdAccessor, AttrIterator> *expr =
        dynamic_cast<const AtomicAttributeExpression<T, DocIdAccessor, AttrIterator> *>(checkExpr);

    if (expr && this->_attributeName == expr->_attributeName && this->getType() == expr->getType() &&
        this->_docIdAccessor == expr->_docIdAccessor) {
        return true;
    }

    return false;
}

} // namespace turing
} // namespace suez
