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

#include <string>

#include "autil/Log.h"
#include "indexlib/index/normal/primarykey/primary_key_index_reader.h"
#include "indexlib/misc/common.h"
#include "indexlib/partition/index_partition.h"

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil
namespace indexlib {
namespace partition {
class JoinInfo;
class PartitionReaderSnapshot;
struct AttributeReaderInfo;
struct AttributeReaderInfoV2;
} // namespace partition
} // namespace indexlib
namespace matchdoc {
class MatchDoc;
class MatchDocAllocator;
class ReferenceBase;
template <typename T>
class Reference;
} // namespace matchdoc

namespace suez {
namespace turing {

class AttributeExpression;
class JoinDocIdConverterCreator;

class AttributeExpressionFactory {
public:
    AttributeExpressionFactory(const std::string &mainTableName,
                               indexlib::partition::PartitionReaderSnapshot *partitionReaderSnapshot,
                               JoinDocIdConverterCreator *joinDocIdConverterCreator,
                               autil::mem_pool::Pool *pool,
                               matchdoc::MatchDocAllocator *allocator,
                               std::string subTableName = "");

    virtual ~AttributeExpressionFactory() {}

public:
    /* virtual for FakeAttributeExpressionFactory */
    virtual AttributeExpression *createAtomicExpr(const std::string &attrName,
                                                  const std::string &prefixName = std::string());
    static AttributeExpression *createAttributeExpression(matchdoc::ReferenceBase *refer, autil::mem_pool::Pool *pool);
    bool getAttributeJoinInfo(const std::string &joinTableName,
                              const std::string &mainTableName,
                              indexlib::partition::JoinInfo &joinInfo) const;

private:
    template <typename T>
    static AttributeExpression *doCreateAttributeExpression(matchdoc::ReferenceBase *refer,
                                                            autil::mem_pool::Pool *pool);

    AttributeExpression *createExpressionForPKAttr();
    AttributeExpression *createNormalAttrExpr(const std::string &attributeName,
                                              const indexlib::partition::AttributeReaderInfo &attributeReaderInfo);
    AttributeExpression *createNormalAttrExpr(const std::string &attributeName,
                                              const indexlib::partition::AttributeReaderInfoV2 &attributeReaderInfo);
    AttributeExpression *createJoinAttrExpr(const std::string &attributeName,
                                            const indexlib::partition::AttributeReaderInfo &attributeReaderInfo,
                                            const indexlib::partition::JoinInfo &joinInfo);
    AttributeExpression *createSubAttrExpr(const std::string &attributeName,
                                           const indexlib::partition::AttributeReaderInfo &attributeReaderInfo);
    AttributeExpression *createReferAttrExpr(const std::string &attributeName);

    template <typename T, typename DocIdAccessor, typename AttributeReaderPtrType = indexlib::index::AttributeReaderPtr>
    AttributeExpression *doCreateAtomicAttrExprTyped(const std::string &attrName,
                                                     const AttributeReaderPtrType &attrReaderPtr,
                                                     const DocIdAccessor &docIdAccessor);

    template <typename DocIdAccessor, typename AttributeReaderPtrType = indexlib::index::AttributeReaderPtr>
    AttributeExpression *createExpressionWithReader(const AttributeReaderPtrType &attributeReaderPtr,
                                                    const std::string &attributeName,
                                                    const DocIdAccessor &docIdAccessor);

private:
    std::string _mainTableName;
    std::string _subTableName;
    indexlib::partition::PartitionReaderSnapshot *_partitionReaderSnapshot;
    JoinDocIdConverterCreator *_joinDocIdConverterCreator;
    matchdoc::Reference<matchdoc::MatchDoc> *_subDocRef;
    autil::mem_pool::Pool *_pool;
    matchdoc::MatchDocAllocator *_allocator;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace turing
} // namespace suez
