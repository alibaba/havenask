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
#ifndef __INDEXLIB_IN_MEMORY_SEGMENT_READER_H
#define __INDEXLIB_IN_MEMORY_SEGMENT_READER_H

#include <memory>

#include "autil/mem_pool/ChunkAllocatorBase.h"
#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/RecyclePool.h"
#include "indexlib/common_define.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(index, AttributeSegmentReader);
DECLARE_REFERENCE_CLASS(index, SummarySegmentReader);
DECLARE_REFERENCE_CLASS(index, InMemSourceSegmentReader);
DECLARE_REFERENCE_CLASS(index, InMemDeletionMapReader);
DECLARE_REFERENCE_CLASS(index, IndexSegmentReader);
DECLARE_REFERENCE_CLASS(index, MultiFieldIndexSegmentReader);
DECLARE_REFERENCE_CLASS(index, KKVSegmentReader);

namespace indexlib { namespace index_base {

class InMemorySegmentReader
{
public:
    typedef std::map<std::string, index::AttributeSegmentReaderPtr> String2AttrReaderMap;
    typedef std::shared_ptr<autil::mem_pool::ChunkAllocatorBase> AllocatorPtr;
    typedef std::shared_ptr<autil::mem_pool::Pool> PoolPtr;
    typedef std::shared_ptr<autil::mem_pool::RecyclePool> RecyclePoolPtr;

public:
    InMemorySegmentReader(segmentid_t segId = INVALID_SEGMENTID);
    virtual ~InMemorySegmentReader();

public:
    void Init(const index::KKVSegmentReaderPtr& buildingKKVSegmentReader, const SegmentInfo& segmentInfo);
    void Init(const index::MultiFieldIndexSegmentReaderPtr& indexSegmentReader, const String2AttrReaderMap& attrReaders,
              const index::SummarySegmentReaderPtr& summaryReader,
              const index::InMemSourceSegmentReaderPtr& sourceReader, const index::InMemDeletionMapReaderPtr& delMap,
              const SegmentInfo& segmentInfo);

    segmentid_t GetSegmentId() const { return mSegmentId; }
    const SegmentInfo& GetSegmentInfo() const { return mSegmentInfo; }

    const index::MultiFieldIndexSegmentReaderPtr& GetMultiFieldIndexSegmentReader() const
    {
        return mIndexSegmentReader;
    }

    virtual index::AttributeSegmentReaderPtr GetAttributeSegmentReader(const std::string& name) const;
    virtual index::SummarySegmentReaderPtr GetSummaryReader() const;
    index::InMemSourceSegmentReaderPtr GetInMemSourceReader() const { return mSourceReader; }
    index::InMemDeletionMapReaderPtr GetInMemDeletionMapReader() const;
    const index::KKVSegmentReaderPtr& GetKKVSegmentReader() const { return mBuildingKKVSegmentReader; };

private:
    segmentid_t mSegmentId;
    index_base::SegmentInfo mSegmentInfo;
    String2AttrReaderMap mAttrReaders;
    index::SummarySegmentReaderPtr mSummaryReader;
    index::InMemSourceSegmentReaderPtr mSourceReader;
    index::InMemDeletionMapReaderPtr mInMemDeletionMapReader;
    index::MultiFieldIndexSegmentReaderPtr mIndexSegmentReader;
    index::KKVSegmentReaderPtr mBuildingKKVSegmentReader;

    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(InMemorySegmentReader);
}} // namespace indexlib::index_base

#endif //__INDEXLIB_IN_MEMORY_SEGMENT_READER_H
