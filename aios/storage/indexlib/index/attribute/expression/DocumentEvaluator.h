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

#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "indexlib/index/attribute/expression/AtomicAttributeExpression.h"

namespace expression {
template <typename T>
class AttributeExpressionTyped;
class AttributeExpression;
} // namespace expression

namespace indexlibv2::index {

class DocumentEvaluatorBase : private autil::NoCopyable
{
public:
    enum EvaluateType {
        ET_UNKNOWN = 0,
        ET_INT8 = 1,
        ET_UINT8 = 2,
        ET_INT16 = 3,
        ET_UINT16 = 4,
        ET_INT32 = 5,
        ET_UINT32 = 6,
        ET_INT64 = 7,
        ET_UINT64 = 8,
        ET_FLOAT = 9,
        ET_DOUBLE = 10,
        ET_STRING = 11,
        ET_BOOL = 12,
    };

public:
    DocumentEvaluatorBase() {}
    virtual ~DocumentEvaluatorBase() {}
    virtual Status Init(const std::vector<AtomicAttributeExpressionBase*>& dependExpressions,
                        const std::shared_ptr<matchdoc::MatchDocAllocator>& allocator,
                        expression::AttributeExpression* _expression) = 0;
    virtual EvaluateType GetEvaluateType() const = 0;

private:
    AUTIL_LOG_DECLARE();
};

template <typename T>
class DocumentEvaluator : public DocumentEvaluatorBase
{
public:
    DocumentEvaluator() : DocumentEvaluatorBase() { _expression = nullptr; }
    ~DocumentEvaluator()
    {
        if (_allocator) {
            _allocator->deallocate(_matchDoc);
        }
    }

public:
    Status Init(const std::vector<AtomicAttributeExpressionBase*>& dependExpressions,
                const std::shared_ptr<matchdoc::MatchDocAllocator>& allocator,
                expression::AttributeExpression* expression) override;
    std::pair<Status, T> Evaluate(docid_t docId);
    EvaluateType GetEvaluateType() const override { return EvaluateType(_expression->getExprValueType()); }

private:
    Status Prefetch(docid_t docId);

private:
    std::vector<AtomicAttributeExpressionBase*> _dependExpressions;
    expression::AttributeExpressionTyped<T>* _expression;
    std::shared_ptr<matchdoc::MatchDocAllocator> _allocator;
    matchdoc::MatchDoc _matchDoc;
    AUTIL_LOG_DECLARE();
};

//////////////////////////////////////////////////////////////////////////////////////////////////////////////

AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, DocumentEvaluator, T);
template <typename T>
Status DocumentEvaluator<T>::Init(const std::vector<AtomicAttributeExpressionBase*>& dependExpressions,
                                  const std::shared_ptr<matchdoc::MatchDocAllocator>& allocator,
                                  expression::AttributeExpression* expression)
{
    assert(allocator);
    assert(expression);
    _dependExpressions = dependExpressions;
    _expression = dynamic_cast<expression::AttributeExpressionTyped<T>*>(expression);
    _allocator = allocator;
    _matchDoc = _allocator->allocate(0);
    if (!_expression) {
        AUTIL_LOG(ERROR, "cast expression type failed, expression [%s] this [%s]", typeid(*expression).name(),
                  typeid(*this).name());
        return Status::InvalidArgs("type cast failed");
    }
    return Status::OK();
}

template <typename T>
std::pair<Status, T> DocumentEvaluator<T>::Evaluate(docid_t docId)
{
    RETURN2_IF_STATUS_ERROR(Prefetch(docId), T(), "prefetch doc [%d] failed", docId);
    _matchDoc.setDocId(docId);
    _expression->bottomUpEvaluate(_matchDoc);
    //    _expression->evaluate(_matchDoc); TODO(xiaohao.yxh) why not evaluate?
    T value = _expression->getValue(_matchDoc);
    return {Status::OK(), value};
}

template <typename T>
Status DocumentEvaluator<T>::Prefetch(docid_t docId)
{
    _matchDoc.setDocId(docId);
    for (auto& expression : _dependExpressions) {
        RETURN_IF_STATUS_ERROR(expression->Prefetch(_matchDoc), "prefetch docid [%d] failed", docId);
    }
    return Status::OK();
}

} // namespace indexlibv2::index
