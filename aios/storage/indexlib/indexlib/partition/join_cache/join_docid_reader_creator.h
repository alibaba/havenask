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
#ifndef __INDEXLIB_JOIN_DOCID_READER_CREATOR_H
#define __INDEXLIB_JOIN_DOCID_READER_CREATOR_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/attribute_iterator_typed.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/index_partition_reader.h"
#include "indexlib/partition/join_cache/join_docid_reader_base.h"
#include "indexlib/partition/join_cache/join_field.h"

DECLARE_REFERENCE_CLASS(partition, IndexPartitionReader);
DECLARE_REFERENCE_CLASS(partition, JoinInfo);

namespace indexlib { namespace partition {

class JoinDocidReaderCreator
{
public:
    typedef index::AttributeIteratorTyped<docid_t> JoinDocidCacheIterator;
    JoinDocidReaderCreator();
    ~JoinDocidReaderCreator();

public:
    static JoinDocidReaderBase* Create(const JoinInfo& joinInfo, autil::mem_pool::Pool* pool);
    static JoinDocidReaderBase* Create(const IndexPartitionReaderPtr& mainIndexPartReader,
                                       const IndexPartitionReaderPtr& joinIndexPartReader, const JoinField& joinField,
                                       uint64_t auxPartitionIdentifier, bool isSubJoin, autil::mem_pool::Pool* pool);

private:
    template <typename JoinAttrType>
    static JoinDocidReaderBase*
    createJoinDocidReaderTyped(const index::AttributeReaderPtr& attrReaderPtr,
                               const std::shared_ptr<index::InvertedIndexReader>& pkIndexReaderPtr,
                               JoinDocidCacheIterator* docIdCacheIter, const index::DeletionMapReaderPtr& delMapReader,
                               const std::string& joinAttrName, bool isSubJoin, autil::mem_pool::Pool* pool);

    static JoinDocidCacheIterator* createJoinDocIdCacheIterator(const IndexPartitionReaderPtr& indexPartReader,
                                                                const std::string& joinAttrName,
                                                                autil::mem_pool::Pool* pool);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(JoinDocidReaderCreator);
}} // namespace indexlib::partition

#endif //__INDEXLIB_JOIN_DOCID_READER_CREATOR_H
