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
#ifndef __INDEXLIB_PARTITION_INFO_H
#define __INDEXLIB_PARTITION_INFO_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index_base/index_meta/partition_meta.h"
#include "indexlib/index_base/index_meta/temperature_doc_info.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/index_define.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(index, DeletionMapReader);
DECLARE_REFERENCE_CLASS(index_base, InMemorySegment);

namespace indexlibv2::table {
class NormalTabletInfoCreator;
}

namespace indexlib { namespace index {

struct PartitionInfoHint {
    segmentid_t lastIncSegmentId;
    segmentid_t lastRtSegmentId;
    segmentid_t needRefindRtSegmentId;
    size_t lastRtSegmentDocCount;

    PartitionInfoHint()
        : lastIncSegmentId(INVALID_SEGMENTID)
        , lastRtSegmentId(INVALID_SEGMENTID)
        , needRefindRtSegmentId(INVALID_SEGMENTID)
        , lastRtSegmentDocCount(0)
    {
    }

    bool operator==(const PartitionInfoHint& other)
    {
        return lastIncSegmentId == other.lastIncSegmentId && lastRtSegmentId == other.lastRtSegmentId &&
               needRefindRtSegmentId == other.needRefindRtSegmentId &&
               lastRtSegmentDocCount == other.lastRtSegmentDocCount;
    }
};

struct PartitionMetrics {
    segmentid_t segmentCount;
    size_t delDocCount;
    // TODO: change type to uint32_t
    size_t totalDocCount;
    size_t incDocCount;

    PartitionMetrics() { Reset(); }

    void Reset()
    {
        segmentCount = 0;
        delDocCount = 0;
        totalDocCount = 0;
        incDocCount = 0;
    }

    bool operator==(const PartitionMetrics& other)
    {
        return segmentCount == other.segmentCount && delDocCount == other.delDocCount &&
               totalDocCount == other.totalDocCount && incDocCount == other.incDocCount;
    }
};

class PartitionInfo;
DEFINE_SHARED_PTR(PartitionInfo);

class PartitionInfo
{
public:
    PartitionInfo() {}
    PartitionInfo(const PartitionInfo& other);

    virtual ~PartitionInfo() {}

public:
    void Init(const index_base::Version& version, const index_base::PartitionMetaPtr& partitionMeta,
              const index_base::SegmentDataVector& segmentDatas,
              const std::vector<index_base::InMemorySegmentPtr>& dumpingSegments,
              const DeletionMapReaderPtr& deletionMapReader);

    const PartitionInfoHint& GetPartitionInfoHint() const { return mPartInfoHint; }

    virtual bool GetDiffDocIdRanges(const PartitionInfoHint& infoHint, DocIdRangeVector& docIdRanges) const;

    bool GetOrderedDocIdRanges(DocIdRangeVector& ranges) const;

    /* now we don't support multi unordered docid range */
    bool GetUnorderedDocIdRange(DocIdRange& range) const;

    bool GetTemperatureDocIdRanges(int32_t hintValues, DocIdRangeVector& ranges) const;
    globalid_t GetGlobalId(docid_t docId) const;
    docid_t GetDocId(globalid_t gid) const;

    segmentid_t GetSegmentId(docid_t docId) const;
    std::pair<segmentid_t, docid_t> GetLocalDocInfo(docid_t docId) const;

    const std::vector<docid_t>& GetBaseDocIds() const { return mBaseDocIds; }
    docid_t GetBaseDocId(segmentid_t segId) const;
    size_t GetTotalDocCount() const { return mPartitionMetrics.totalDocCount; }
    segmentid_t GetSegmentCount() const { return mPartitionMetrics.segmentCount; }

    index_base::PartitionMetaPtr GetPartitionMeta() const { return mPartMeta; }

    bool NeedUpdate(const index_base::InMemorySegmentPtr& inMemSegment) const;
    void UpdateInMemorySegment(const index_base::InMemorySegmentPtr& inMemSegment);
    void AddInMemorySegment(const index_base::InMemorySegmentPtr& inMemSegment);

    const index_base::TemperatureDocInfoPtr GetTemperatureDocInfo() const { return mTemperatureDocRange; }

    virtual PartitionInfo* Clone();

    void SetSubPartitionInfo(const PartitionInfoPtr& subPartitionInfo) { mSubPartitionInfo = subPartitionInfo; }
    const PartitionInfoPtr& GetSubPartitionInfo() const { return mSubPartitionInfo; }

    const PartitionMetrics& GetPartitionMetrics() const { return mPartitionMetrics; }

    const index_base::Version& GetVersion() const { return mVersion; }

public:
    // for test
    void SetPartitionInfoHint(const PartitionInfoHint& infoHint) { mPartInfoHint = infoHint; }
    void SetBaseDocIds(const std::vector<docid_t>& baseDocIds) { mBaseDocIds = baseDocIds; }
    void SetVersion(const index_base::Version& version) { mVersion = version; }
    void SetOrderedDocIdRanges(const DocIdRangeVector& ranges) { mOrderRanges = ranges; }
    void SetUnOrderedDocIdRange(const DocIdRange& range) { mUnorderRange = range; }
    void SetDeletionMapReader(const DeletionMapReaderPtr& deletionMapReaderPtr)
    {
        mDeletionMapReader = deletionMapReaderPtr;
    }
    void SetPartitionMeta(const index_base::PartitionMetaPtr& meta) { mPartMeta = meta; }
    void TEST_SetTotalDocCount(size_t totalDocCount) { mPartitionMetrics.totalDocCount = totalDocCount; }
    PartitionMetrics& GetPartitionMetrics() { return mPartitionMetrics; }
    void TEST_AssertEqual(const PartitionInfo& other);
    void TEST_SetTemperatureDocRange(const std::map<TemperatureProperty, DocIdRangeVector>& meta)
    {
        mTemperatureDocRange.reset(new index_base::TemperatureDocInfo(meta));
    }

private:
    void InitVersion(const std::vector<index_base::InMemorySegmentPtr>& dumpingSegments);
    void InitBaseDocIds(const index_base::SegmentDataVector& segmentDatas,
                        const std::vector<index_base::InMemorySegmentPtr>& dumpingSegments);
    void InitPartitionMetrics(const index_base::SegmentDataVector& segmentDatas,
                              const std::vector<index_base::InMemorySegmentPtr>& dumpingSegments);
    void InitOrderedDocIdRanges(const index_base::SegmentDataVector& segmentDatas);
    void InitUnorderedDocIdRange();
    void InitPartitionInfoHint(const index_base::SegmentDataVector& segmentDatas,
                               const std::vector<index_base::InMemorySegmentPtr>& dumpingSegments);

    docid_t GetSegmentDocCount(size_t idx) const;

    void InitTemperatureMetaInfo(const index_base::Version& version, const index_base::SegmentDataVector& segmentDatas,
                                 const std::vector<index_base::InMemorySegmentPtr>& dumpingSegments);
    bool GetIncDiffDocIdRange(segmentid_t lastIncSegId, DocIdRange& range) const;
    bool GetRtDiffDocIdRange(segmentid_t lastRtSegId, size_t lastRtSegDocCount, DocIdRange& range) const;

    bool IsKeyValueTable() const { return !mDeletionMapReader; }
    DocIdRangeVector CombineDocIdRange(const DocIdRangeVector& currentRange, const DocIdRangeVector& range) const;

protected:
    std::vector<docid_t> mBaseDocIds;
    std::map<segmentid_t, docid_t> mSegIdToBaseDocId;
    index_base::Version mVersion;
    PartitionInfoHint mPartInfoHint;
    std::vector<DocIdRange> mOrderRanges;
    DocIdRange mUnorderRange;
    index_base::PartitionMetaPtr mPartMeta;
    DeletionMapReaderPtr mDeletionMapReader;
    PartitionMetrics mPartitionMetrics;
    PartitionInfoPtr mSubPartitionInfo;
    index_base::TemperatureDocInfoPtr mTemperatureDocRange;

private:
    friend class indexlibv2::table::NormalTabletInfoCreator;
    friend class PartitionInfoTest;
    IE_LOG_DECLARE();
};
}} // namespace indexlib::index

#endif //__INDEXLIB_PARTITION_INFO_H
