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

#include <algorithm>
#include <functional>
#include <memory>

#include "autil/Log.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/index/DocMapper.h"
#include "indexlib/index/deletionmap/DeletionMapDiskIndexer.h"
#include "indexlib/index/deletionmap/DeletionMapIndexReader.h"
#include "indexlib/table/index_task/merger/SegmentMergePlan.h"
#include "indexlib/table/normal_table/index_task/SegmentMapper.h"

namespace indexlibv2 { namespace table {

// ReclaimMap记录一个SegmentMergePlan中doc旧docid和新docid的映射关系
// TODO (远轫) 因二进制兼容问题，暂不支持64位docid
class ReclaimMap : public index::DocMapper
{
public:
    typedef std::function<std::pair<Status, segmentindex_t>(segmentid_t, docid_t)> SegmentSplitHandler;

protected:
    class State
    {
    public:
        State(size_t targetSegmentCount, const SegmentSplitHandler& segmentSplitHandler, uint32_t maxDocCount);
        State(bool multiTargetSegment, uint32_t maxDocCount); // for sub
        ~State();

    public:
        Status ReclaimOneDoc(docid_t baseDocId, segmentid_t segId,
                             const std::shared_ptr<indexlibv2::index::DeletionMapDiskIndexer>& deletionmapDiskIndexer,
                             docid32_t localId);
        void FillReclaimMap(const std::vector<std::pair<docid_t, std::shared_ptr<framework::Segment>>>& srcSegments,
                            uint32_t& deletedDocCount, int64_t& totalDocCount, std::vector<docid_t>& oldDocIdToNewDocId,
                            std::vector<docid_t>& targetSegments);

    protected:
        void RewriteDocIdArray(const std::vector<std::pair<docid_t, std::shared_ptr<framework::Segment>>>& srcSegments,
                               const SegmentMapper& segMapper);
        void MakeTargetBaseDocIds(const SegmentMapper& segMapper, segmentindex_t maxSegIdx);
        bool NeedSplitSegment() const { return mSegmentSplitHandler.operator bool(); }

    protected:
        SegmentSplitHandler mSegmentSplitHandler;

        SegmentMapper _segMapper;

        // may use reclaim_map directly.
        uint32_t _deletedDocCount;
        docid_t _newDocId;
        std::vector<docid_t> _docIdArray;
        std::vector<docid_t> _targetBaseDocIds;
        // DocOrderPtr mDocIdOrder;
    };

public:
    ReclaimMap(std::string name, framework::IndexTaskResourceType type) : index::DocMapper(std::move(name), type) {}

    ~ReclaimMap() {}

public:
    Status Init(const SegmentMergePlan& segMergePlan, const std::shared_ptr<framework::TabletData>& tabletData,
                const SegmentSplitHandler& segmentSplitHandler);
    std::pair<segmentid_t, docid32_t> Map(docid64_t globalOldDoc) const override;
    std::pair<segmentid_t, docid32_t> ReverseMap(docid64_t newDocId) const override;

    // return docid in plan
    docid64_t GetNewId(docid64_t oldId) const override
    {
        assert(oldId < (docid_t)_oldDocIdToNewDocId.size());
        return _oldDocIdToNewDocId[oldId];
    }

    int32_t GetTargetSegmentCount() { return _targetSegmentIds.size(); }
    int64_t GetTargetSegmentDocCount(segmentid_t segmentId) const override;

    uint32_t GetDeletedDocCount() const { return _deletedDocCount; }
    uint32_t GetNewDocCount() const override { return _totalDocCount; }

    Status Store(const std::shared_ptr<indexlib::file_system::Directory>& resourceDirectory) override;

    Status Load(const std::shared_ptr<indexlib::file_system::Directory>& resourceDirectory) override;

    segmentid_t GetLocalId(docid64_t newId) const override;

public:
    // for test
    Status TEST_Init(segmentid_t baseSegId,
                     const std::vector<std::pair<docid_t, std::shared_ptr<framework::Segment>>>& srcSegments,
                     const SegmentSplitHandler& segmentSplitHandler)
    {
        return Calculate(0, baseSegId, srcSegments, segmentSplitHandler);
    }

    int64_t TEST_GetOriginDocCount() { return _oldDocIdToNewDocId.size(); }

protected:
    void
    CalculateReverseDocMap(const std::vector<std::pair<docid_t, std::shared_ptr<framework::Segment>>>& srcSegments);
    void CollectSrcSegments(const SegmentMergePlan& segMergePlan,
                            const std::shared_ptr<framework::TabletData>& tabletData,
                            std::vector<std::pair<docid_t, std::shared_ptr<framework::Segment>>>& srcSegments);
    std::pair<docid_t, std::shared_ptr<framework::Segment>>
    GetSourceSegment(segmentid_t srcSegmentId, const std::shared_ptr<framework::TabletData>& tabletData);
    template <typename T>
    bool StoreVector(const indexlib::file_system::FileWriterPtr& writer, const std::vector<T>& vec) const;

    template <typename T>
    bool LoadVector(indexlib::file_system::FileReaderPtr& reader, std::vector<T>& vec);

    Status Calculate(size_t targetSegmentCount, segmentid_t baseSegmentId,
                     const std::vector<std::pair<docid_t, std::shared_ptr<framework::Segment>>>& srcSegments,
                     const SegmentSplitHandler& segmentSplitHandler);
    segmentid_t GetTargetSegmentId(int32_t targetSegmentIdx) const { return _targetSegmentIds[targetSegmentIdx]; }
    segmentid_t GetTargetSegmentIndex(docid_t newId) const;

protected:
    uint32_t _deletedDocCount = 0;
    int64_t _totalDocCount = 0;               // all doc count without delete doc
    std::vector<docid_t> _oldDocIdToNewDocId; // 数组下标是oldGlobalDocId，值是新的docid
    std::vector<docid_t> _targetSegmentBaseDocIds;
    std::vector<segmentid_t> _targetSegmentIds;
    std::vector<std::pair<segmentid_t, docid_t>> _newDocIdToOldDocid;

private:
    AUTIL_LOG_DECLARE();
};

}} // namespace indexlibv2::table
