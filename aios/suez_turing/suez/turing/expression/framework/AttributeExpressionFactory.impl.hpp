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
#include <stdint.h>
#include <cstddef>
#include <memory>
#include <tr1/type_traits>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/LongHashValue.h"
#include "autil/MultiValueType.h"
#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/PoolBase.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader.h"
#include "indexlib/index/normal/primarykey/primary_key_index_reader.h"
#include "indexlib/index/attribute/AttributeIteratorTyped.h"
#include "indexlib/index/attribute/AttributeReader.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/index_partition_reader.h"
#include "indexlib/partition/join_cache/join_info.h"
#include "indexlib/partition/partition_define.h"
#include "indexlib/partition/partition_reader_snapshot.h"
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
}  // namespace index
}  // namespace indexlib
namespace suez {
namespace turing {
class JoinDocIdConverterBase;
}  // namespace turing
}  // namespace suez

using namespace std;
using namespace std::tr1;
using namespace autil;
using namespace indexlib::index;
using namespace indexlib::partition;

namespace suez::turing {

template <typename T, typename DocIdAccessor, typename AttributeReaderPtrType>
AttributeExpression *AttributeExpressionFactory::doCreateAtomicAttrExprTyped(
    const string &attrName, const AttributeReaderPtrType &attrReaderPtr, const DocIdAccessor &docIdAccessor) {
    typedef AtomicAttributeExpression<T, DocIdAccessor> AtomicAttributeExpressionTyped;
    typedef typename AtomicAttributeExpression<T, DocIdAccessor>::Iterator Iterator;

    typedef AtomicAttributeExpression<
        T,
        DocIdAccessor,
        indexlibv2::index::AttributeIteratorTyped<T, indexlibv2::index::AttributeReaderTraits<T>>>
        AtomicAttributeExpressionTypedV2;
    typedef typename AtomicAttributeExpressionTypedV2::Iterator IteratorV2;

    auto *attrIterBase = attrReaderPtr->CreateIterator(_pool);
    assert(attrIterBase);

    Iterator *attrIter = dynamic_cast<Iterator *>(attrIterBase);
    if (NULL != attrIter) {
        return POOL_NEW_CLASS(_pool, AtomicAttributeExpressionTyped, attrName, attrIter, docIdAccessor);
    } else {
        IteratorV2 *attrIterV2 = dynamic_cast<IteratorV2 *>(attrIterBase);
        if (NULL != attrIterV2) {
            return POOL_NEW_CLASS(_pool, AtomicAttributeExpressionTypedV2, attrName, attrIterV2, docIdAccessor);
        }
    }
    return NULL;
}

} // namespace suez::turing

#define IMPL_ONE_ATOMIC_ATTR_EXPR_TYPED(VT, T_DOC_ID_ACCESSOR, T_ATTR_READER)                                          \
    template AttributeExpression *AttributeExpressionFactory::                                                         \
        doCreateAtomicAttrExprTyped<VariableTypeTraits<VT, true>::AttrExprType, T_DOC_ID_ACCESSOR, T_ATTR_READER>(     \
            const string &, const T_ATTR_READER &, const T_DOC_ID_ACCESSOR &);                                         \
    template AttributeExpression *AttributeExpressionFactory::                                                         \
        doCreateAtomicAttrExprTyped<VariableTypeTraits<VT, false>::AttrExprType, T_DOC_ID_ACCESSOR, T_ATTR_READER>(    \
            const string &, const T_ATTR_READER &, const T_DOC_ID_ACCESSOR &)
