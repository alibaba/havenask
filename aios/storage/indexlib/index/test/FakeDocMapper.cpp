#include "indexlib/index/test/FakeDocMapper.h"

#include <cassert>

#include "autil/StringUtil.h"
#include "indexlib/framework/Segment.h"

namespace indexlibv2::index {

FakeDocMapper::FakeDocMapper(std::string name, framework::IndexTaskResourceType type) : DocMapper(std::move(name), type)
{
}

void FakeDocMapper::Init(const std::vector<SrcSegmentInfo>& srcSegInfos, segmentid_t firstTargetSegId,
                         uint32_t targetSegCount, bool isRoundRobin)
{
    assert(!srcSegInfos.empty());
    _firstTargetSegId = firstTargetSegId;
    ValidateDeletedDocs(srcSegInfos);
    CalcNewDocCount(srcSegInfos);
    _oldDocIdToNew.resize(srcSegInfos.back().baseDocId + srcSegInfos.back().docCount, INVALID_DOCID);
    CalcTargetDocCounts(targetSegCount);
    if (isRoundRobin) {
        InitRoundRobin(srcSegInfos);
    } else {
        InitNormal(srcSegInfos);
    }
}

void FakeDocMapper::Init(segmentid_t firstTargetSegId, const std::vector<SrcSegmentInfo>& srcSegInfos,
                         const SegmentSplitHandler& segmentSplitHandler)
{
    assert(!srcSegInfos.empty());
    _firstTargetSegId = firstTargetSegId;
    ValidateDeletedDocs(srcSegInfos);
    CalcNewDocCount(srcSegInfos);
    _oldDocIdToNew.resize(srcSegInfos.back().baseDocId + srcSegInfos.back().docCount, INVALID_DOCID);
    std::vector<uint32_t> targetSegBaseDocIds;
    std::map<std::pair<segmentid_t, docid_t>, segmentindex_t> docMapper;
    CalcTargetDocInfos(srcSegInfos, segmentSplitHandler, targetSegBaseDocIds, docMapper);
    std::vector<uint32_t> targetDocIdInfos;
    targetDocIdInfos.resize(targetSegBaseDocIds.size());
    for (size_t i = 0; i < srcSegInfos.size(); ++i) {
        const auto& info = srcSegInfos[i];
        for (size_t j = 0; j < info.docCount; ++j) {
            if (info.deletedDocs.find(j) != info.deletedDocs.end()) {
                continue;
            }
            auto segId = info.segId;
            docid_t localDocId = j;
            segmentindex_t targetSegId = docMapper[std::make_pair(segId, localDocId)];
            docid_t oldDocId = info.baseDocId + j;
            docid_t targetSegBaseDocId = targetSegBaseDocIds[targetSegId];
            docid_t newLocalDocId = targetDocIdInfos[targetSegId];
            targetDocIdInfos[targetSegId] += 1;
            docid_t newDocId = targetSegBaseDocId + newLocalDocId;
            _oldDocIdToNew[oldDocId] = newDocId;
            _oldToNew[oldDocId] = std::make_pair(_firstTargetSegId + targetSegId, newLocalDocId);
            _newToOld[newDocId] = std::make_pair(info.segId, j);
        }
    }
}

void FakeDocMapper::Init(const std::vector<SrcSegmentInfo>& srcSegInfos,
                         const indexlibv2::index::IIndexMerger::SegmentMergeInfos& segmentMergeInfos,
                         const std::string& docMapStr)
{
    // old {0,1,2,3} --> new {0,4,5}, input {1,2,3} --> output {4,5}
    std::map<std::pair<segmentid_t, docid_t>, std::pair<segmentid_t, docid_t>> old2NewMap;
    std::map<std::pair<segmentid_t, docid_t>, std::pair<segmentid_t, docid_t>> new2OldMap;
    std::map<segmentid_t, docid_t> outputDocCounts;
    // 1:0>4:0, 1:1>5:1, 2:0>D, 2:1>5:0, 3:0>4:1
    std::vector<std::string> docMapStrVec;
    autil::StringUtil::fromString(docMapStr, docMapStrVec, ",");
    for (const auto& oneDoc : docMapStrVec) {
        std::vector<std::vector<std::string>> oneDocMap;
        autil::StringUtil::fromString(oneDoc, oneDocMap, ":", ">");
        assert(oneDocMap.size() == 2);    // oldDoc>newDoc
        assert(oneDocMap[0].size() == 2); // oldSegid:oldLocalDocId
        auto oldSegId = autil::StringUtil::numberFromString<segmentid_t>(oneDocMap[0][0]);
        docid_t oldLocalDocId = autil::StringUtil::numberFromString<docid_t>(oneDocMap[0][1]);
        if (oneDocMap[1].size() == 2) { // >newSegId:newLocalDocId
            segmentid_t newSegId = autil::StringUtil::numberFromString<segmentid_t>(oneDocMap[1][0]);
            docid_t newLocalDocId = autil::StringUtil::numberFromString<docid_t>(oneDocMap[1][1]);
            old2NewMap[std::make_pair(oldSegId, oldLocalDocId)] = std::make_pair(newSegId, newLocalDocId);
            new2OldMap[std::make_pair(newSegId, newLocalDocId)] = std::make_pair(oldSegId, oldLocalDocId);
            outputDocCounts[newSegId]++;
        } else { // >DELETE
            assert(oneDocMap[1].size() == 0);
            old2NewMap[std::make_pair(oldSegId, oldLocalDocId)] = std::make_pair(INVALID_SEGMENTID, INVALID_DOCID);
        }
    }
    // inputSegIdSet, segments set that wiil be merged
    std::set<segmentid_t> inputSegIdSet;
    for (const auto& srcSegment : segmentMergeInfos.srcSegments) {
        inputSegIdSet.insert(srcSegment.segment->GetSegmentId());
    }
    // outputSegIdSet, segment set that merged output
    for (const auto& targetSegmentMeta : segmentMergeInfos.targetSegments) {
        targetSegmentMeta->segmentInfo->docCount = outputDocCounts[targetSegmentMeta->segmentId];
    }
    // generate docid map
    docid_t newGlobalDocId = 0;
    std::map<segmentid_t, docid_t> oldBaseDocIds;
    std::map<segmentid_t, docid_t> newBaseDocIds;
    // keep segment: {0}
    for (const auto& srcSeg : srcSegInfos) {
        const auto& [segId, docCount, baseDocId, deletedDocs] = srcSeg;
        _oldDocIdToNew.resize(_oldDocIdToNew.size() + docCount);
        oldBaseDocIds[segId] = baseDocId;
        if (inputSegIdSet.count(segId) == 0) {
            newBaseDocIds[segId] = newGlobalDocId;
            for (docid_t oldLocalDocId = 0; oldLocalDocId < docCount; ++oldLocalDocId) {
                auto oldDocId = baseDocId + oldLocalDocId;
                auto newDocId = newGlobalDocId++;
                _oldDocIdToNew[oldDocId] = newDocId;
                _oldToNew[oldDocId] = std::make_pair(segId, oldLocalDocId);
                _newToOld[newDocId] = std::make_pair(segId, oldLocalDocId);
            }
        }
    }
    // output segment: {4,5}
    for (const auto& [newSegId, docCount] : outputDocCounts) {
        newBaseDocIds[newSegId] = newGlobalDocId;
        for (docid_t newLocalDocId = 0; newLocalDocId < docCount; ++newLocalDocId) {
            const auto& [oldSegId, oldLocalDocId] = new2OldMap.at(std::make_pair(newSegId, newLocalDocId));
            auto oldDocId = oldBaseDocIds[oldSegId] + oldLocalDocId;
            auto newDocId = newGlobalDocId++;
            _oldDocIdToNew[oldDocId] = newDocId;
            _oldToNew[oldDocId] = std::make_pair(newSegId, newLocalDocId);
            _newToOld[newDocId] = std::make_pair(oldSegId, oldLocalDocId);
        }
    }
    // drop segment: {1,2,3}
    for (const auto& srcSeg : srcSegInfos) {
        const auto& [oldSegId, docCount, baseDocId, deletedDocs] = srcSeg;
        if (inputSegIdSet.count(oldSegId) > 0) {
            for (docid_t oldLocalDocId = 0; oldLocalDocId < docCount; ++oldLocalDocId) {
                auto oldDocId = baseDocId + oldLocalDocId;
                const auto& [newSegId, newLocalDocId] = old2NewMap.at(std::make_pair(oldSegId, oldLocalDocId));
                auto newDocId =
                    (newLocalDocId == INVALID_DOCID) ? INVALID_DOCID : newBaseDocIds[newSegId] + newLocalDocId;
                _oldDocIdToNew[oldDocId] = newDocId;
                _oldToNew[oldDocId] = std::make_pair(newSegId, newLocalDocId);
                _newToOld[newDocId] = std::make_pair(oldSegId, oldLocalDocId);
            }
        }
    }
    _newDocCount = newGlobalDocId;
}

void FakeDocMapper::ValidateDeletedDocs(const std::vector<SrcSegmentInfo>& srcSegInfos)
{
    for (size_t i = 0; i < srcSegInfos.size(); ++i) {
        auto& info = srcSegInfos[i];
        for (auto iter = info.deletedDocs.begin(); iter != info.deletedDocs.end(); ++iter) {
            assert(*iter >= 0);
            assert(*iter < info.docCount);
        }
    }
}

void FakeDocMapper::InitNormal(const std::vector<SrcSegmentInfo>& srcSegInfos)
{
    docid_t newDocId = 0;
    size_t targetSegIdx = 0;
    docid_t newBaseDocId = 0;
    for (size_t i = 0; i < srcSegInfos.size(); ++i) {
        const auto& info = srcSegInfos[i];
        for (size_t j = 0; j < info.docCount; ++j) {
            if (info.deletedDocs.find(j) != info.deletedDocs.end()) {
                continue;
            }
            docid_t oldDocId = info.baseDocId + j;
            docid_t newLocalDocId = newDocId - newBaseDocId;
            _oldDocIdToNew[oldDocId] = newDocId;
            _oldToNew[oldDocId] = std::make_pair(_firstTargetSegId + targetSegIdx, newLocalDocId);
            _newToOld[newDocId] = std::make_pair(info.segId, j);
            if (newLocalDocId + 1 >= _targetDocCounts[targetSegIdx]) {
                ++targetSegIdx;
                newBaseDocId += newLocalDocId + 1;
            }
            newDocId++;
        }
    }
}

void FakeDocMapper::InitRoundRobin(const std::vector<SrcSegmentInfo>& srcSegInfos)
{
    size_t targetSegIdx = 0;
    docid_t newBaseDocId = 0;
    size_t rowIdx = 0;
    std::vector<int> idxs(srcSegInfos.size(), 0);
    for (size_t i = 0; i < _newDocCount; ++i) {
        while (true) {
            bool find = false;
            auto& info = srcSegInfos[rowIdx];
            while (idxs[rowIdx] < info.docCount) {
                docid_t oldDocId = info.baseDocId + idxs[rowIdx];
                docid_t oldLocalId = idxs[rowIdx];
                if (info.deletedDocs.find(oldLocalId) == info.deletedDocs.end()) {
                    find = true;
                    docid_t newDocId = i;
                    docid_t newLocalDocId = newDocId - newBaseDocId;
                    _oldDocIdToNew[oldDocId] = newDocId;
                    _oldToNew[oldDocId] = std::make_pair(_firstTargetSegId + targetSegIdx, newLocalDocId);
                    _newToOld[newDocId] = std::make_pair(info.segId, oldLocalId);
                    if (newLocalDocId + 1 >= _targetDocCounts[targetSegIdx]) {
                        ++targetSegIdx;
                        newBaseDocId += newLocalDocId + 1;
                    }
                    ++idxs[rowIdx];
                    break;
                } else {
                    ++idxs[rowIdx];
                }
            }
            rowIdx = (rowIdx + 1) % idxs.size();
            if (find) {
                break;
            }
        }
    }
}

void FakeDocMapper::CalcTargetDocCounts(uint32_t targetSegCount)
{
    assert(targetSegCount >= 1);
    std::vector<uint32_t> docCounts;
    uint32_t countPerSeg = _newDocCount / targetSegCount;
    for (size_t i = 0; i < targetSegCount - 1; ++i) {
        docCounts.emplace_back(countPerSeg);
    }
    docCounts.emplace_back(_newDocCount - (targetSegCount - 1) * countPerSeg);
    _targetDocCounts.swap(docCounts);
}

void FakeDocMapper::CalcTargetDocInfos(const std::vector<SrcSegmentInfo>& srcSegInfos,
                                       const SegmentSplitHandler& segmentSplitHandler,
                                       std::vector<uint32_t>& targetSegBaseDocIds,
                                       std::map<std::pair<segmentid_t, docid_t>, segmentindex_t>& docMapper)
{
    for (size_t i = 0; i < srcSegInfos.size(); ++i) {
        const auto& info = srcSegInfos[i];
        for (size_t j = 0; j < info.docCount; ++j) {
            auto segId = info.segId;
            docid_t localDocId = j;
            segmentindex_t targetSegId = 0;
            if (segmentSplitHandler.operator bool()) {
                targetSegId = segmentSplitHandler(segId, localDocId);
            }
            docMapper[std::make_pair(segId, localDocId)] = targetSegId;
            if (targetSegId >= _targetDocCounts.size()) {
                _targetDocCounts.resize(targetSegId + 1);
            }
            if (info.deletedDocs.find(j) != info.deletedDocs.end()) {
                continue;
            }
            _targetDocCounts[targetSegId] += 1;
        }
    }
    targetSegBaseDocIds.resize(_targetDocCounts.size());
    uint32_t baseDocId = 0;
    for (size_t i = 0; i < _targetDocCounts.size(); i++) {
        targetSegBaseDocIds[i] = baseDocId;
        baseDocId += _targetDocCounts[i];
    }
}

void FakeDocMapper::CalcNewDocCount(const std::vector<SrcSegmentInfo>& srcSegInfos)
{
    for (size_t i = 0; i < srcSegInfos.size(); ++i) {
        const auto& info = srcSegInfos[i];
        uint32_t docCount = info.docCount;
        uint32_t deletedCount = info.deletedDocs.size();
        assert(docCount >= deletedCount);
        _newDocCount += docCount - deletedCount;
    }
}

std::pair<segmentid_t, docid32_t> FakeDocMapper::Map(docid64_t oldDocId) const
{
    auto iter = _oldToNew.find(oldDocId);
    if (iter != _oldToNew.end()) {
        return iter->second;
    }
    return std::make_pair(INVALID_SEGMENTID, INVALID_DOCID);
}
std::pair<segmentid_t, docid32_t> FakeDocMapper::ReverseMap(docid64_t newDocId) const
{
    auto iter = _newToOld.find(newDocId);
    if (iter != _newToOld.end()) {
        return iter->second;
    }
    return std::make_pair(INVALID_SEGMENTID, INVALID_DOCID);
}

docid64_t FakeDocMapper::GetNewId(docid64_t oldId) const
{
    assert(oldId >= 0);
    assert(oldId < _oldDocIdToNew.size());
    return _oldDocIdToNew[oldId];
}

Status FakeDocMapper::Store(const std::shared_ptr<indexlib::file_system::Directory>& resourceDirectory)
{
    assert(false);
    return {};
}
Status FakeDocMapper::Load(const std::shared_ptr<indexlib::file_system::Directory>& resourceDirectory)
{
    assert(false);
    return {};
}

} // namespace indexlibv2::index
