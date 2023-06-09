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
#include "indexlib/index/attribute/expression/AtomicExpressionCreator.h"

#include "autil/mem_pool/Pool.h"
#include "expression/framework/AttributeExpressionPool.h"
#include "indexlib/base/FieldTypeUtil.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/index/attribute/Common.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/attribute/expression/AtomicAttributeExpression.h"
#include "indexlib/index/attribute/expression/TabletSessionResource.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.table, AtomicExpressionCreator);
using expression::AttributeExpression;
using expression::AttributeExpressionPool;

AtomicExpressionCreator::AtomicExpressionCreator(const std::vector<std::shared_ptr<framework::Segment>>& segments,
                                                 const std::shared_ptr<config::TabletSchema>& schema,
                                                 expression::AttributeExpressionPool* exprPool,
                                                 autil::mem_pool::Pool* pool)
    : _segments(segments)
    , _schema(schema)
    , _exprPool(exprPool)
    , _pool(pool)
{
    assert(schema);
}

AtomicExpressionCreator::~AtomicExpressionCreator() {}

AttributeExpression* AtomicExpressionCreator::createAtomicExpr(const std::string& name, const std::string& tableName)
{
    if (!tableName.empty() && tableName != _schema->GetTableName()) {
        AUTIL_LOG(ERROR, "table name[%s] is diff from schema[%s]", tableName.c_str(), _schema->GetTableName().c_str());
        return nullptr;
    }
    AtomicAttributeExpressionBase* atomicExprBase = nullptr;
    auto expr = _exprPool->tryGetAttributeExpression(name);
    if (!expr) {
        auto attrConfig = std::dynamic_pointer_cast<config::AttributeConfig>(
            _schema->GetIndexConfig(index::ATTRIBUTE_INDEX_TYPE_STR, name));
        if (!attrConfig) {
            AUTIL_LOG(ERROR, "attribute[%s] is not exist", name.c_str());
            return nullptr;
        }

#define CASE(ft)                                                                                                       \
    case ft: {                                                                                                         \
        if (attrConfig->IsMultiValue()) {                                                                              \
            typedef indexlib::IndexlibFieldType2CppType<ft, true>::CppType CppType;                                    \
            AtomicAttributeExpression<CppType>* typedExpr =                                                            \
                POOL_NEW_CLASS(_pool, AtomicAttributeExpression<CppType>, attrConfig);                                 \
            atomicExprBase = typedExpr;                                                                                \
            expr = typedExpr;                                                                                          \
        } else {                                                                                                       \
            typedef indexlib::IndexlibFieldType2CppType<ft, false>::CppType CppType;                                   \
            AtomicAttributeExpression<CppType>* typedExpr =                                                            \
                POOL_NEW_CLASS(_pool, AtomicAttributeExpression<CppType>, attrConfig);                                 \
            atomicExprBase = typedExpr;                                                                                \
            expr = typedExpr;                                                                                          \
        }                                                                                                              \
        break;                                                                                                         \
    }

        // TODO(xiaohao.yxh) fieldtype->att type-> T
        switch (attrConfig->GetFieldType()) {
            CASE(FieldType::ft_int8);
            CASE(FieldType::ft_int16);
            CASE(FieldType::ft_int32);
            CASE(FieldType::ft_int64);
            CASE(FieldType::ft_uint8);
            CASE(FieldType::ft_uint16);
            CASE(FieldType::ft_uint32);
            CASE(FieldType::ft_uint64);
            CASE(FieldType::ft_double);
            CASE(FieldType::ft_float);
            CASE(FieldType::ft_time);
            CASE(FieldType::ft_date);
            CASE(FieldType::ft_timestamp);
            CASE(FieldType::ft_string);
        default:
            AUTIL_LOG(ERROR, "create expression type [%s] [%d] [%d]/not support, field name[%s]",
                      indexlibv2::config::FieldConfig::FieldTypeToStr(attrConfig->GetFieldType()),
                      attrConfig->GetFieldType(), FieldType::ft_int64, name.c_str());
            return nullptr;
        }
        if (!expr) {
            AUTIL_LOG(ERROR, "create expression failed, name[%s]", name.c_str());
            return nullptr;
        }
        std::vector<expression::expressionid_t> exprIds;
        exprIds.push_back((expression::expressionid_t)_exprPool->getAttributeExpressionCount());
        _uniqCreatedExpressions.push_back(atomicExprBase);
        expr->init(exprIds, _exprPool);
        _exprPool->addPair(name, expr);
    }
    _createdExpressions.push_back(atomicExprBase);
    return expr;
}

void AtomicExpressionCreator::endRequest() {}
bool AtomicExpressionCreator::beginRequest(std::vector<expression::AttributeExpression*>& allExprInSession,
                                           expression::SessionResource* resourceBase)
{
    auto resource = dynamic_cast<TabletSessionResource*>(resourceBase);
    if (!resource) {
        AUTIL_LOG(ERROR, "cast resource type failed");
        return false;
    }
    for (auto& singleExpression : _uniqCreatedExpressions) {
        auto status = singleExpression->InitPrefetcher(resource);
        if (!status.IsOK()) {
            AUTIL_LOG(ERROR, "init prefercher failed, code[%s]", status.ToString().c_str());
            return false;
        }
    }
    return true;
}

size_t AtomicExpressionCreator::GetCreatedExpressionCount() const { return _createdExpressions.size(); }
AtomicAttributeExpressionBase* AtomicExpressionCreator::GetCreatedExpression(size_t index) const
{
    assert(index < _createdExpressions.size());
    return _createdExpressions[index];
}

} // namespace indexlibv2::index
