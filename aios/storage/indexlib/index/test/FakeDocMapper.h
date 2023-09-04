#pragma once
#include "indexlib/base/Constant.h"
#include "indexlib/base/Types.h"
#include "indexlib/index/DocMapper.h"
#include "indexlib/index/IIndexMerger.h"

namespace indexlibv2::index {

struct SrcSegmentInfo {
    SrcSegmentInfo() : segId(INVALID_SEGMENTID), docCount(0), baseDocId(INVALID_DOCID) {}
    SrcSegmentInfo(segmentid_t segId, uint32_t docCount, docid_t baseDocId, const std::set<docid_t>& deletedDocs)
        : segId(segId)
        , docCount(docCount)
        , baseDocId(baseDocId)
        , deletedDocs(deletedDocs)
    {
    }
    segmentid_t segId;
    uint32_t docCount;
    docid_t baseDocId;
    std::set<docid_t> deletedDocs; // local docid, start from 0
};

class FakeDocMapper : public DocMapper
{
public:
    FakeDocMapper(std::string name, framework::IndexTaskResourceType type);
    ~FakeDocMapper() = default;

    using SegmentSplitHandler = std::function<segmentindex_t(segmentid_t, docid_t)>;

public:
    // isRoundRobin: for example as beblow, segment0 + segment1 -> segment2
    //               local_docid(field_value)
    //               segment0: 0(a) 1(b)
    //               segment1: 0(h) 1(i)
    //               isRoundRobin=true  segment2: 0(a) 1(h) 2(b) 3(i)
    //               isRoundRobin=false segment2: 0(a) 1(b) 2(h) 3(i)
    // targetSegCount: when bigger than 1, split as balanced as possible
    void Init(const std::vector<SrcSegmentInfo>& srcSegInfos, segmentid_t firstTargetSegId, uint32_t targetSegCount = 1,
              bool isRoundRobin = false);
    void Init(segmentid_t firstTargetSegId, const std::vector<SrcSegmentInfo>& srcSegInfos,
              const SegmentSplitHandler& segmentSplitHandler);
    // srcSegInfos: all segments in base version
    // docMapStr = oldSegId:oldDocId>newSegId:newDocId, oldSegId:oldDocId>:,...
    void Init(const std::vector<SrcSegmentInfo>& srcSegInfos,
              const indexlibv2::index::IIndexMerger::SegmentMergeInfos& segmentMergeInfos,
              const std::string& docMapStr);

    // oldGlobalDocId -> [newSegId, newLocalDocId]
    std::pair<segmentid_t, docid_t> Map(docid_t oldDocId) const override;
    // newGlobalDocId -> [oldSegId, oldLocalDocId]
    std::pair<segmentid_t, docid_t> ReverseMap(docid_t newDocId) const override;
    segmentid_t GetTargetSegmentId(int32_t targetSegmentIdx) const override { return -1; }
    int64_t GetTargetSegmentDocCount(int32_t idx) const override
    {
        assert(idx < _targetDocCounts.size());
        return _targetDocCounts[idx];
    }

    // oldGlobalDocId -> newGlobalDocId
    docid_t GetNewId(docid_t oldId) const override;

    // new total doc count
    uint32_t GetNewDocCount() const override { return _newDocCount; }

    Status Store(const std::shared_ptr<indexlib::file_system::Directory>& resourceDirectory) override;
    Status Load(const std::shared_ptr<indexlib::file_system::Directory>& resourceDirectory) override;

    segmentid_t GetLocalId(docid_t newId) const override { return -1; }
    segmentid_t GetTargetSegmentIndex(docid_t newId) const override { return -1; }

private:
    void CalcNewDocCount(const std::vector<SrcSegmentInfo>& srcSegInfos);
    void CalcTargetDocCounts(uint32_t targetSegCount);
    void CalcTargetDocInfos(const std::vector<SrcSegmentInfo>& srcSegInfos,
                            const SegmentSplitHandler& segmentSplitHandler, std::vector<uint32_t>& targetSegBaseDocIds,
                            std::map<std::pair<segmentid_t, docid_t>, segmentindex_t>& docMapper);
    void InitNormal(const std::vector<SrcSegmentInfo>& srcSegInfos);
    void InitRoundRobin(const std::vector<SrcSegmentInfo>& srcSegInfos);
    void ValidateDeletedDocs(const std::vector<SrcSegmentInfo>& srcSegInfos);

private:
    std::map<docid_t, std::pair<segmentid_t, docid_t>> _oldToNew;
    std::map<docid_t, std::pair<segmentid_t, docid_t>> _newToOld;
    std::vector<docid_t> _oldDocIdToNew;
    uint32_t _newDocCount = 0;
    std::vector<uint32_t> _targetDocCounts;
    segmentid_t _firstTargetSegId = INVALID_SEGMENTID;
};

} // namespace indexlibv2::index
