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
#include "indexlib/table/normal_table/index_task/ReclaimMap.h"

#include "indexlib/file_system/Directory.h"
#include "indexlib/index/deletionmap/Common.h"

namespace indexlibv2 { namespace table {
AUTIL_LOG_SETUP(indexlib.table, ReclaimMap);

ReclaimMap::State::State(size_t targetSegmentCount, const SegmentSplitHandler& segmentSplitHandler,
                         uint32_t maxDocCount)
    : mSegmentSplitHandler(segmentSplitHandler)
    , _deletedDocCount(0)
    , _newDocId(0)
{
    _docIdArray.resize(maxDocCount, INVALID_DOCID);
    if (NeedSplitSegment()) {
        _segMapper.Init(maxDocCount, true, targetSegmentCount);
    }
}

ReclaimMap::State::~State() {}

Status ReclaimMap::State::ReclaimOneDoc(
    docid_t baseDocId, segmentid_t segId,
    const std::shared_ptr<indexlibv2::index::DeletionMapDiskIndexer>& deletionmapDiskIndexer, docid_t localId)
{
    if (deletionmapDiskIndexer && deletionmapDiskIndexer->IsDeleted(localId)) {
        ++_deletedDocCount;
        return Status::OK();
    }

    auto oldGlobalId = baseDocId + localId;
    if (NeedSplitSegment()) {
        auto [status, segIndex] = mSegmentSplitHandler(segId, localId);
        RETURN_IF_STATUS_ERROR(status, "split failed, segment id [%d], local doc id [%d]", segId, localId);
        auto currentSegDocCount = _segMapper.GetSegmentDocCount(segIndex);
        _docIdArray[oldGlobalId] = static_cast<docid_t>(currentSegDocCount);
        _segMapper.Collect(oldGlobalId, segIndex);
    } else {
        _docIdArray[oldGlobalId] = _newDocId++;
    }
    return Status::OK();
}

void ReclaimMap::State::FillReclaimMap(
    const std::vector<std::pair<docid_t, std::shared_ptr<framework::Segment>>>& srcSegments, uint32_t& deletedDocCount,
    int64_t& totalDocCount, std::vector<docid_t>& oldDocIdToNewDocId, std::vector<docid_t>& targetSegments)
{
    if (NeedSplitSegment()) {
        MakeTargetBaseDocIds(_segMapper, _segMapper.GetMaxSegmentIndex());
        RewriteDocIdArray(srcSegments, _segMapper);
    } else {
        _targetBaseDocIds.push_back(0);
    }
    deletedDocCount = _deletedDocCount;
    totalDocCount = _newDocId;
    oldDocIdToNewDocId.swap(_docIdArray);
    targetSegments.swap(_targetBaseDocIds);
}

void ReclaimMap::State::MakeTargetBaseDocIds(const SegmentMapper& segMapper, segmentindex_t maxSegIdx)
{
    size_t currentDocs = 0;
    _targetBaseDocIds.clear();
    _targetBaseDocIds.reserve(maxSegIdx + 1);
    for (segmentindex_t segIdx = 0; segIdx <= maxSegIdx; ++segIdx) {
        _targetBaseDocIds.push_back(currentDocs);
        currentDocs += segMapper.GetSegmentDocCount(segIdx);
    }
}

void ReclaimMap::State::RewriteDocIdArray(
    const std::vector<std::pair<docid_t, std::shared_ptr<framework::Segment>>>& srcSegments,
    const SegmentMapper& segMapper)
{
    _newDocId = 0;
    for (const auto& srcSegment : srcSegments) {
        for (size_t localId = 0; localId < srcSegment.second->GetSegmentInfo()->docCount; ++localId) {
            auto oldId = srcSegment.first + localId;
            if (_docIdArray[oldId] == INVALID_DOCID) {
                continue;
            }
            auto segIdx = segMapper.GetSegmentIndex(oldId);
            _docIdArray[oldId] += _targetBaseDocIds[segIdx];
            ++_newDocId;
        }
    }
}

Status ReclaimMap::Init(const SegmentMergePlan& segMergePlan, const std::shared_ptr<framework::TabletData>& tabletData,
                        const SegmentSplitHandler& segmentSplitHandler)
{
    int64_t before = autil::TimeUtility::currentTimeInSeconds();

    std::vector<std::pair<docid_t, std::shared_ptr<framework::Segment>>> srcSegments;
    CollectSrcSegments(segMergePlan, tabletData, srcSegments);
    assert(segMergePlan.GetTargetSegmentCount() > 0);
    auto baseSegmentId = segMergePlan.GetTargetSegmentId(0);
    auto status = Calculate(segMergePlan.GetTargetSegmentCount(), baseSegmentId, srcSegments, segmentSplitHandler);
    if (!status.IsOK()) {
        return status;
    }
    int64_t after = autil::TimeUtility::currentTimeInSeconds();
    AUTIL_LOG(INFO, "Init reclaim map use %ld seconds", (after - before));
    return Status::OK();
}

Status ReclaimMap::Calculate(size_t targetSegmentCount, segmentid_t baseSegmentId,
                             const std::vector<std::pair<docid_t, std::shared_ptr<framework::Segment>>>& srcSegments,
                             const SegmentSplitHandler& segmentSplitHandler)
{
    uint32_t maxDocCount = srcSegments.rbegin()->first + srcSegments.rbegin()->second->GetSegmentInfo()->docCount;
    State state(targetSegmentCount, segmentSplitHandler, maxDocCount);
    for (size_t i = 0; i < srcSegments.size(); ++i) {
        auto [baseDocId, segment] = srcSegments[i];
        auto [status, indexer] = segment->GetIndexer(indexlibv2::index::DELETION_MAP_INDEX_TYPE_STR,
                                                     indexlibv2::index::DELETION_MAP_INDEX_NAME);
        std::shared_ptr<indexlibv2::index::DeletionMapDiskIndexer> deletionmapDiskIndexer;
        if (status.IsOK()) {
            deletionmapDiskIndexer = std::dynamic_pointer_cast<indexlibv2::index::DeletionMapDiskIndexer>(indexer);
        } else {
            if (!status.IsNotFound()) {
                return status;
            }
        }
        uint32_t segDocCount = segment->GetSegmentInfo()->docCount;
        for (uint32_t j = 0; j < segDocCount; ++j) {
            RETURN_IF_STATUS_ERROR(state.ReclaimOneDoc(baseDocId, segment->GetSegmentId(), deletionmapDiskIndexer, j),
                                   "reclaim one doc failed");
        }
    }
    state.FillReclaimMap(srcSegments, _deletedDocCount, _totalDocCount, _oldDocIdToNewDocId, _targetSegmentBaseDocIds);
    _targetSegmentIds.clear();
    for (size_t i = 0; i < _targetSegmentBaseDocIds.size(); i++) {
        _targetSegmentIds.push_back(baseSegmentId++);
    }
    // caculate reverse
    CalculateReverseDocMap(srcSegments);
    return Status::OK();
}

void ReclaimMap::CalculateReverseDocMap(
    const std::vector<std::pair<docid_t, std::shared_ptr<framework::Segment>>>& srcSegments)
{
    _newDocIdToOldDocid.resize(_totalDocCount);
    for (size_t i = 0; i < srcSegments.size(); ++i) {
        auto [baseDocId, segment] = srcSegments[i];
        uint32_t segDocCount = segment->GetSegmentInfo()->docCount;
        for (uint32_t j = 0; j < segDocCount; ++j) {
            auto newDocId = _oldDocIdToNewDocId[baseDocId + j];
            if (newDocId != INVALID_DOCID) {
                _newDocIdToOldDocid[newDocId] = std::make_pair(segment->GetSegmentId(), j);
            }
        }
    }
}

void ReclaimMap::CollectSrcSegments(const SegmentMergePlan& segMergePlan,
                                    const std::shared_ptr<framework::TabletData>& tabletData,
                                    std::vector<std::pair<docid_t, std::shared_ptr<framework::Segment>>>& srcSegments)
{
    for (size_t i = 0; i < segMergePlan.GetSrcSegmentCount(); i++) {
        auto segId = segMergePlan.GetSrcSegmentId(i);
        srcSegments.push_back(GetSourceSegment(segId, tabletData));
    }
}

int64_t ReclaimMap::GetTargetSegmentDocCount(int32_t idx) const
{
    assert(_targetSegmentBaseDocIds.size() == _targetSegmentIds.size());
    assert(idx < _targetSegmentBaseDocIds.size());
    if (idx == _targetSegmentBaseDocIds.size() - 1) {
        return _totalDocCount - _targetSegmentBaseDocIds[idx];
    }
    return _targetSegmentBaseDocIds[idx + 1] - _targetSegmentBaseDocIds[idx];
}

void ReclaimMap::GetOldDocIdAndSegId(docid_t newDocId, docid_t& oldDocId, segmentid_t& oldSegId) const
{
    assert(newDocId < _totalDocCount);
    oldDocId = _newDocIdToOldDocid[newDocId].second;
    oldSegId = _newDocIdToOldDocid[newDocId].first;
}

std::pair<docid_t, std::shared_ptr<framework::Segment>>
ReclaimMap::GetSourceSegment(segmentid_t srcSegmentId, const std::shared_ptr<framework::TabletData>& tabletData)
{
    docid_t baseDocId = 0;
    auto slice = tabletData->CreateSlice(framework::Segment::SegmentStatus::ST_UNSPECIFY);
    for (auto iter = slice.begin(); iter != slice.end(); iter++) {
        if (srcSegmentId == (*iter)->GetSegmentId()) {
            return std::make_pair(baseDocId, *iter);
        }
        baseDocId += (*iter)->GetSegmentInfo()->docCount;
    }
    return std::make_pair(INVALID_DOCID, nullptr);
}

std::pair<segmentid_t, docid_t> ReclaimMap::Map(docid_t globalOldDocId) const
{
    assert(globalOldDocId < _oldDocIdToNewDocId.size());
    auto newDocId = _oldDocIdToNewDocId[globalOldDocId];
    if (newDocId == INVALID_DOCID) {
        return std::make_pair(INVALID_SEGMENTID, INVALID_DOCID);
    }
    for (size_t i = 0; i < _targetSegmentBaseDocIds.size() - 1; i++) {
        if (newDocId < _targetSegmentBaseDocIds[i + 1]) {
            return std::make_pair(_targetSegmentIds[i], newDocId - _targetSegmentBaseDocIds[i]);
        }
    }
    auto lastSegmentIdx = _targetSegmentBaseDocIds.size() - 1;
    return std::make_pair(_targetSegmentIds[lastSegmentIdx], newDocId - _targetSegmentBaseDocIds[lastSegmentIdx]);
}

segmentid_t ReclaimMap::GetLocalId(docid_t newId) const
{
    if (newId == INVALID_DOCID) {
        return INVALID_SEGMENTID;
    }
    auto segIdx = GetTargetSegmentIndex(newId);
    return _targetSegmentIds[segIdx];
}

segmentid_t ReclaimMap::GetTargetSegmentIndex(docid_t newId) const
{
    if (_targetSegmentBaseDocIds.empty()) {
        return 0;
    }
    auto iter = std::upper_bound(_targetSegmentBaseDocIds.begin(), _targetSegmentBaseDocIds.end(), newId) - 1;
    return iter - _targetSegmentBaseDocIds.begin();
}

Status ReclaimMap::Store(const std::shared_ptr<indexlib::file_system::Directory>& resourceDirectory)
{
    try {
        indexlib::file_system::FileWriterPtr writer = resourceDirectory->CreateFileWriter(_name);
        writer->Write(&_deletedDocCount, sizeof(_deletedDocCount)).GetOrThrow();
        writer->Write(&_totalDocCount, sizeof(_totalDocCount)).GetOrThrow();
        if (!StoreVector(writer, _oldDocIdToNewDocId)) {
            AUTIL_LOG(ERROR, "store old to new with io error");
            return Status::IOError("store old to new failed");
        }
        if (!StoreVector(writer, _targetSegmentBaseDocIds)) {
            AUTIL_LOG(ERROR, "store target segment base docid  with io error");
            return Status::IOError("store target segment base docid failed");
        }
        if (!StoreVector(writer, _targetSegmentIds)) {
            AUTIL_LOG(ERROR, "store target segment ids failed");
            return Status::IOError("store target segmentids failed");
        }
        if (!StoreVector(writer, _newDocIdToOldDocid)) {
            AUTIL_LOG(ERROR, "store new to old failed");
            return Status::IOError("store new to old failed");
        }
        writer->Close().GetOrThrow();
    } catch (const autil::legacy::ExceptionBase& e) {
        AUTIL_LOG(ERROR, "store reclaimmap failed, exception [%s]", e.what());
        return Status::IOError("store reclaim map failed");
    }
    return Status::OK();
}

std::pair<segmentid_t, docid_t> ReclaimMap::ReverseMap(docid_t newDocId) const
{
    assert(newDocId < _newDocIdToOldDocid.size());
    return _newDocIdToOldDocid[newDocId];
}

Status ReclaimMap::Load(const std::shared_ptr<indexlib::file_system::Directory>& resourceDirectory)
{
    try {
        indexlib::file_system::FileReaderPtr reader =
            resourceDirectory->CreateFileReader(_name, indexlib::file_system::FSOpenType::FSOT_BUFFERED);
        if (reader->Read(&_deletedDocCount, sizeof(_deletedDocCount)).GetOrThrow() != sizeof(_deletedDocCount)) {
            AUTIL_LOG(ERROR, "read deleted doc count failed");
            return Status::IOError();
        }
        if (reader->Read(&_totalDocCount, sizeof(_totalDocCount)).GetOrThrow() != sizeof(_totalDocCount)) {
            AUTIL_LOG(ERROR, "read reclaimmap failed");
            return Status::IOError("load reclaim map failed");
        }
        if (!LoadVector(reader, _oldDocIdToNewDocId)) {
            AUTIL_LOG(ERROR, "load oldDocIdToNewDocId failed")
            return Status::IOError("load reclaim map failed");
        }
        if (!LoadVector(reader, _targetSegmentBaseDocIds)) {
            AUTIL_LOG(ERROR, "load targetSegmentBaseDocIds failed")
            return Status::IOError("load reclaim map failed");
        }
        if (!LoadVector(reader, _targetSegmentIds)) {
            AUTIL_LOG(ERROR, "load targetSegmentIds failed")
            return Status::IOError("load reclaim map failed");
        }
        if (!LoadVector(reader, _newDocIdToOldDocid)) {
            AUTIL_LOG(ERROR, "load reverse docid map failed")
            return Status::IOError("load reclaim map failed");
        }
    } catch (const autil::legacy::ExceptionBase& e) {
        AUTIL_LOG(ERROR, "load reclaimmap failed, exception [%s]", e.what());
        return Status::IOError("load reclaim map failed");
    }
    return Status::OK();
}

template <typename T>
bool ReclaimMap::LoadVector(indexlib::file_system::FileReaderPtr& reader, std::vector<T>& vec)
{
    uint32_t size = 0;
    if (reader->Read(&size, sizeof(size)).GetOrThrow() != sizeof(size)) {
        AUTIL_LOG(ERROR, "read vector size failed");
        return false;
    }
    if (size > 0) {
        vec.resize(size);
        int64_t expectedSize = sizeof(vec[0]) * size;
        auto readSize = reader->Read(vec.data(), expectedSize).GetOrThrow();
        if (readSize != (size_t)expectedSize) {
            AUTIL_LOG(ERROR, "read vector data failed");
            return false;
        }
    }
    return true;
}

template <typename T>
bool ReclaimMap::StoreVector(const indexlib::file_system::FileWriterPtr& writer, const std::vector<T>& vec) const
{
    uint32_t size = vec.size();
    writer->Write(&size, sizeof(size)).GetOrThrow();
    if (size <= 0) {
        assert(size == 0);
        return true;
    }
    writer->Write(vec.data(), sizeof(vec[0]) * size).GetOrThrow();
    return true;
}

}} // namespace indexlibv2::table
