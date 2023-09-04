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
#include "suez/turing/expression/framework/AttributeExpressionFactory.h"

#include <assert.h>
#include <cstddef>
#include <memory>
#include <stdint.h>
#include <tr1/type_traits>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/LongHashValue.h"
#include "autil/MultiValueType.h"
#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/PoolBase.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index/attribute/AttributeIteratorTyped.h"
#include "indexlib/index/attribute/AttributeReader.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader.h"
#include "indexlib/index/normal/primarykey/legacy_primary_key_reader_interface.h"
#include "indexlib/index/normal/primarykey/primary_key_index_reader.h"
#include "indexlib/index/primary_key/PrimaryKeyIndexReader.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/index_partition_reader.h"
#include "indexlib/partition/join_cache/join_info.h"
#include "indexlib/partition/partition_define.h"
#include "indexlib/partition/partition_reader_snapshot.h"
#include "indexlib/table/normal_table/NormalTabletSessionReader.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/Reference.h"
#include "matchdoc/ValueType.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/framework/AtomicAttributeExpression.h"
#include "suez/turing/expression/framework/AttributeExpression.h"
#include "suez/turing/expression/framework/DocIdAccessor.h"
#include "suez/turing/expression/framework/JoinDocIdConverterCreator.h"
#include "suez/turing/expression/framework/VariableTypeTraits.h"

namespace indexlib {
namespace index {
class AttributeIteratorBase;
} // namespace index
} // namespace indexlib
namespace suez {
namespace turing {
class JoinDocIdConverterBase;
} // namespace turing
} // namespace suez

using namespace std;
using namespace std::tr1;
using namespace autil;
using namespace indexlib::index;
using namespace indexlib::partition;

namespace suez {
namespace turing {

AUTIL_LOG_SETUP(expression, AttributeExpressionFactory);

AttributeExpressionFactory::AttributeExpressionFactory(
    const std::string &mainTableName,
    indexlib::partition::PartitionReaderSnapshot *partitionReaderSnapshot,
    JoinDocIdConverterCreator *joinDocIdConverterCreator,
    autil::mem_pool::Pool *pool,
    matchdoc::MatchDocAllocator *allocator,
    std::string subTableName)
    : _mainTableName(mainTableName)
    , _subTableName(subTableName)
    , _partitionReaderSnapshot(partitionReaderSnapshot)
    , _joinDocIdConverterCreator(joinDocIdConverterCreator)
    , _subDocRef(allocator ? allocator->getCurrentSubDocRef() : NULL)
    , _pool(pool)
    , _allocator(allocator) {}

AttributeExpression *AttributeExpressionFactory::createExpressionForPKAttr() {
    DefaultDocIdAccessor docIdAccessor;
    if (auto indexPartReader = _partitionReaderSnapshot->GetIndexPartitionReader(_mainTableName)) {
        if (auto pkIndexReader = indexPartReader->GetPrimaryKeyReader()) {
            const auto &legacyPkReader =
                std::dynamic_pointer_cast<indexlib::index::LegacyPrimaryKeyReaderInterface>(pkIndexReader);
            assert(legacyPkReader);
            return createExpressionWithReader(
                legacyPkReader->GetLegacyPKAttributeReader(), PRIMARYKEY_REF, docIdAccessor);
        }
    }
    if (auto tabletReader = _partitionReaderSnapshot->GetTabletReader(_mainTableName)) {
        if (auto normalTabletReader =
                std::dynamic_pointer_cast<indexlibv2::table::NormalTabletSessionReader>(tabletReader)) {
            if (auto pkIndexReader = normalTabletReader->GetPrimaryKeyReader()) {
                return createExpressionWithReader(pkIndexReader->GetPKAttributeReader(), PRIMARYKEY_REF, docIdAccessor);
            }
        }
    }
    return NULL;
}

AttributeExpression *AttributeExpressionFactory::createAtomicExpr(const string &attributeName,
                                                                  const string &prefixName) {
    if (!_partitionReaderSnapshot) {
        return NULL;
    }
    if (attributeName == PRIMARYKEY_REF) {
        return createExpressionForPKAttr();
    }
    std::string tableName = prefixName.empty()
                                ? _partitionReaderSnapshot->GetTableNameByAttribute(attributeName, _mainTableName)
                                : prefixName;
    if (tableName.empty()) {
        AUTIL_LOG(DEBUG, "attribute [%s] not in main table [%s]", attributeName.c_str(), _mainTableName.c_str());
        if (!_subTableName.empty()) {
            tableName = _partitionReaderSnapshot->GetTableNameByAttribute(attributeName, _subTableName);
            if (tableName.empty()) {
                AUTIL_LOG(DEBUG, "attribute [%s] not in sub table [%s]", attributeName.c_str(), _subTableName.c_str());
                return NULL;
            }
        } else {
            return NULL;
        }
    }
    // TODO support pack attribute
    IndexPartitionReaderInfo indexPartReaderInfo;
    bool ret = _partitionReaderSnapshot->GetIndexPartitionReaderInfo(attributeName, tableName, indexPartReaderInfo);
    if (ret) {
        auto tableType = indexPartReaderInfo.indexPartReader->GetSchema()->GetTableType();
        if (tableType == indexlib::tt_kv || tableType == indexlib::tt_kkv) {
            return createReferAttrExpr(attributeName);
        }
    }

    // TODO: add support for tablet (kv/kkv)

    AttributeReaderInfo attributeReaderInfo;
    ret = _partitionReaderSnapshot->GetAttributeReaderInfoV1(attributeName, tableName, attributeReaderInfo);
    if (ret) {
        IndexPartitionReaderType readerType = attributeReaderInfo.indexPartReaderType;
        if (readerType == READER_PRIMARY_MAIN_TYPE) {
            if (_mainTableName.empty()) {
                return createReferAttrExpr(attributeName);
            } else {
                if (tableName == _mainTableName) {
                    return createNormalAttrExpr(attributeName, attributeReaderInfo);
                }
                JoinInfo joinInfo;
                if (getAttributeJoinInfo(tableName, _mainTableName, joinInfo)) {
                    return createJoinAttrExpr(attributeName, attributeReaderInfo, joinInfo);
                }
            }
        }
        if (readerType == READER_PRIMARY_SUB_TYPE) {
            return createSubAttrExpr(attributeName, attributeReaderInfo);
        }
    }

    // pk and kv/kkv not support currently in new framework Tablet
    AttributeReaderInfoV2 attributeReaderInfoV2;
    ret = _partitionReaderSnapshot->GetAttributeReaderInfoV2(attributeName, tableName, attributeReaderInfoV2);
    if (ret) {
        IndexPartitionReaderType readerType = attributeReaderInfoV2.indexPartReaderType;
        if (readerType == READER_PRIMARY_MAIN_TYPE) {
            assert(!_mainTableName.empty());
            return createNormalAttrExpr(attributeName, attributeReaderInfoV2);
        }
    }

    return NULL;
}

bool AttributeExpressionFactory::getAttributeJoinInfo(const string &joinTableName,
                                                      const string &mainTableName,
                                                      JoinInfo &joinInfo) const {
    return _partitionReaderSnapshot->GetAttributeJoinInfo(joinTableName, mainTableName, joinInfo);
}

AttributeExpression *AttributeExpressionFactory::createNormalAttrExpr(const string &attributeName,
                                                                      const AttributeReaderInfo &attributeReaderInfo) {
    DefaultDocIdAccessor docIdAccessor;
    return createExpressionWithReader<DefaultDocIdAccessor>(
        attributeReaderInfo.attrReader, attributeName, docIdAccessor);
}

AttributeExpression *
AttributeExpressionFactory::createNormalAttrExpr(const string &attributeName,
                                                 const AttributeReaderInfoV2 &attributeReaderInfo) {
    DefaultDocIdAccessor docIdAccessor;
    typedef std::shared_ptr<indexlibv2::index::AttributeReader> AttributeReaderPtr;
    return createExpressionWithReader<DefaultDocIdAccessor, AttributeReaderPtr>(
        attributeReaderInfo.attrReader, attributeName, docIdAccessor);
}

AttributeExpression *AttributeExpressionFactory::createReferAttrExpr(const string &attributeName) {
    matchdoc::ReferenceBase *ref = _allocator->findReferenceWithoutType(attributeName);
    if (ref == NULL) {
        return NULL;
    }
    return createAttributeExpression(ref, _pool);
}

AttributeExpression *AttributeExpressionFactory::createJoinAttrExpr(const string &attributeName,
                                                                    const AttributeReaderInfo &attributeReaderInfo,
                                                                    const JoinInfo &joinInfo) {
    assert(_joinDocIdConverterCreator);

    JoinDocIdConverterBase *converter = _joinDocIdConverterCreator->createJoinDocIdConverter(joinInfo);
    if (!converter) {
        AUTIL_LOG(WARN, "create JoinDocIdConverter for join field [%s] failed!", joinInfo.GetJoinFieldName().c_str());
        return NULL;
    }
    JoinDocIdAccessor docIdAccessor(converter);
    AttributeExpression *expr =
        createExpressionWithReader<JoinDocIdAccessor>(attributeReaderInfo.attrReader, attributeName, docIdAccessor);
    expr->setIsSubExpression(joinInfo.IsSubJoin());
    return expr;
}

AttributeExpression *AttributeExpressionFactory::createSubAttrExpr(const string &attributeName,
                                                                   const AttributeReaderInfo &attributeReaderInfo) {
    if (_subDocRef == NULL) {
        AUTIL_LOG(WARN, "create sub attribute expression [%s] is not allow", attributeName.c_str());
        return NULL;
    }
    SubDocIdAccessor subDocIdAccessor(_subDocRef);
    AttributeExpression *expr =
        createExpressionWithReader<SubDocIdAccessor>(attributeReaderInfo.attrReader, attributeName, subDocIdAccessor);
    expr->setIsSubExpression(true);
    return expr;
}

template <typename DocIdAccessor, typename AttributeReaderPtrType>
AttributeExpression *AttributeExpressionFactory::createExpressionWithReader(const AttributeReaderPtrType &attrReaderPtr,
                                                                            const string &attrName,
                                                                            const DocIdAccessor &docIdAccessor) {
    if (!attrReaderPtr.get()) {
        return NULL;
    }

    AttributeExpression *expr = NULL;
    AttrType attrType = attrReaderPtr->GetType();
    auto vt = attrType2VariableType(attrType);
    bool isMulti = attrReaderPtr->IsMultiValue();

#define ATTR_TYPE_CASE_HELPER(vt)                                                                                      \
    case vt:                                                                                                           \
        if (isMulti) {                                                                                                 \
            typedef VariableTypeTraits<vt, true>::AttrExprType AttrExprType;                                           \
            expr = doCreateAtomicAttrExprTyped<AttrExprType, DocIdAccessor, AttributeReaderPtrType>(                   \
                attrName, attrReaderPtr, docIdAccessor);                                                               \
        } else {                                                                                                       \
            typedef VariableTypeTraits<vt, false>::AttrExprType AttrExprType;                                          \
            expr = doCreateAtomicAttrExprTyped<AttrExprType, DocIdAccessor, AttributeReaderPtrType>(                   \
                attrName, attrReaderPtr, docIdAccessor);                                                               \
        }                                                                                                              \
        break

    switch (vt) {
        NUMERIC_VARIABLE_TYPE_MACRO_HELPER(ATTR_TYPE_CASE_HELPER);
        ATTR_TYPE_CASE_HELPER(vt_string);
        ATTR_TYPE_CASE_HELPER(vt_hash_128);
    default:
        AUTIL_LOG(WARN, "unsupport variable type[%d] for attribute", (int32_t)vt);
        break;
    }

#undef ATTR_TYPE_CASE_HELPER
    if (!expr) {
        AUTIL_LOG(WARN,
                  "create AtomicAttributeExpression error, "
                  "attribute name [%s]",
                  attrName.c_str());
    }

    return expr;
}

template <typename T>
AttributeExpression *AttributeExpressionFactory::doCreateAttributeExpression(matchdoc::ReferenceBase *refer,
                                                                             autil::mem_pool::Pool *pool) {
    auto vr = dynamic_cast<matchdoc::Reference<T> *>(refer);
    assert(vr);
    auto expr = POOL_NEW_CLASS(pool, AttributeExpressionTyped<T>);
    expr->setReference(vr);
    return expr;
}

AttributeExpression *AttributeExpressionFactory::createAttributeExpression(matchdoc::ReferenceBase *refer,
                                                                           autil::mem_pool::Pool *pool) {
    auto valueType = refer->getValueType();
    auto vt = valueType.getBuiltinType();
    auto isMulti = valueType.isMultiValue();

#define CREATE_ATTRIBUTE_EXPRESSION_HELPER(vt_type)                                                                    \
    case vt_type: {                                                                                                    \
        if (isMulti) {                                                                                                 \
            typedef VariableTypeTraits<vt_type, true>::AttrItemType T;                                                 \
            return doCreateAttributeExpression<T>(refer, pool);                                                        \
        } else {                                                                                                       \
            typedef VariableTypeTraits<vt_type, false>::AttrItemType T;                                                \
            return doCreateAttributeExpression<T>(refer, pool);                                                        \
        }                                                                                                              \
    } break

    switch (vt) {
        NUMERIC_VARIABLE_TYPE_MACRO_HELPER_WITH_BOOL(CREATE_ATTRIBUTE_EXPRESSION_HELPER);
    case vt_string: {
        if (valueType.isStdType()) {
            AUTIL_LOG(WARN, "unsupport support std string");
            return NULL;
        }
        return doCreateAttributeExpression<autil::MultiChar>(refer, pool);
    }
    default:
        assert(false);
        break;
    }
    return NULL;
}

} // namespace turing
} // namespace suez
