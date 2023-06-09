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
#include "indexlib/index/merger_util/reclaim_map/sub_reclaim_map_creator.h"

#include "indexlib/index/normal/attribute/accessor/attribute_segment_reader.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
#include "indexlib/index/util/segment_directory_base.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/segment/in_memory_segment_reader.h"
#include "indexlib/index_base/segment_mapper.h"
#include "indexlib/util/class_typed_factory.h"

using namespace std;
using namespace indexlib::util;
using namespace indexlib::config;
using namespace indexlib::index_base;
namespace indexlib { namespace index {
IE_LOG_SETUP(index, SubReclaimMapCreator);

SubReclaimMapCreator::State::State(uint32_t maxDocCount, uint32_t mergedDocCount, ReclaimMap* mainReclaimMap,
                                   const vector<int>& segIdxMap, vector<SegmentReaderPair>& mainJoinAttrSegReaders)
    : ReclaimMapCreator::State(SubReclaimMapCreator::MultiTargetSegment(mainReclaimMap), maxDocCount)
    , mMainJoinValues(mainReclaimMap->GetNewDocCount())
    , mMainReclaimMap(mainReclaimMap)
    , mSegIdxMap(segIdxMap)
    , mMainJoinAttrSegReaders(mainJoinAttrSegReaders)
    , mMergedDocCount(mergedDocCount)
{
    mJoinValueIdArray.reserve(mMergedDocCount);
}

SubReclaimMapCreator::State::~State() {}

void SubReclaimMapCreator::State::ReclaimOneDoc(segmentid_t segId, docid_t baseDocId, docid_t localMainDocId,
                                                docid_t globalMainDocId, const DeletionMapReaderPtr& deletionMapReader)
{
    int32_t segIdx = mSegIdxMap[segId];
    SegmentReaderPair& localSegReaderPair = mMainJoinAttrSegReaders[segIdx];

    docid_t lastDocJoinValue = 0;
    docid_t curDocJoinValue = 0;
    GetMainJoinValues(localSegReaderPair, localMainDocId, lastDocJoinValue, curDocJoinValue);

    for (docid_t subDocId = lastDocJoinValue; subDocId < curDocJoinValue; subDocId++) {
        if (deletionMapReader->IsDeleted(segId, subDocId)) {
            continue;
        }
        if (SubReclaimMapCreator::MultiTargetSegment(mMainReclaimMap)) {
            auto targetSegIdx = mMainReclaimMap->GetTargetSegmentIndex(globalMainDocId);
            mSegMapper.Collect(baseDocId + subDocId, targetSegIdx);
        }
        mDocIdArray[baseDocId + subDocId] = mNewDocId++;
        mJoinValueIdArray.push_back(globalMainDocId);
    }
    mMainJoinValues[globalMainDocId] = mNewDocId;
}

ReclaimMapPtr SubReclaimMapCreator::State::ConstructSubReclaimMap(const index_base::SegmentMergeInfos& segMergeInfos,
                                                                  bool needReserveMapping)
{
    if (SubReclaimMapCreator::MultiTargetSegment(mMainReclaimMap)) {
        MakeTargetBaseDocIds(mSegMapper, (segmentindex_t)mMainReclaimMap->GetTargetSegmentCount() - 1);
        mSegMapper.Clear();
    }

    assert(mNewDocId == (docid_t)mJoinValueIdArray.size());
    mDeletedDocCount = mMergedDocCount - mNewDocId;
    mMainReclaimMap->InitJoinValueArray(mMainJoinValues);
    ReclaimMapPtr reclaimMap(new ReclaimMap(mDeletedDocCount, mNewDocId, std::move(mDocIdArray),
                                            std::move(mJoinValueIdArray), std::move(mTargetBaseDocIds)));
    if (!reclaimMap) {
        INDEXLIB_FATAL_ERROR(OutOfMemory, "Allocate memory FAILED.");
    }
    if (needReserveMapping) {
        reclaimMap->InitReverseDocIdArray(segMergeInfos);
    }
    return reclaimMap;
}

void SubReclaimMapCreator::State::GetMainJoinValues(SegmentReaderPair& segReaderPair, docid_t localDocId,
                                                    docid_t& lastDocJoinValue, docid_t& curDocJoinValue) const
{
    assert(segReaderPair.first);
    assert(localDocId >= 0);
    bool isNull = false;
    // TODO support

    // TODO yiping.typ support keep state
    if (localDocId == 0) {
        lastDocJoinValue = 0;
    } else {
        segReaderPair.first->Read(localDocId - 1, lastDocJoinValue, isNull, segReaderPair.second);
    }
    segReaderPair.first->Read(localDocId, curDocJoinValue, isNull, segReaderPair.second);
}

SubReclaimMapCreator::SubReclaimMapCreator(bool hasTruncate, const AttributeConfigPtr& mainJoinAttrConfig)
    : ReclaimMapCreator(hasTruncate)
    , mMainJoinAttrConfig(mainJoinAttrConfig)
{
    assert(mMainJoinAttrConfig->GetAttrName() == MAIN_DOCID_TO_SUB_DOCID_ATTR_NAME);
}

SubReclaimMapCreator::~SubReclaimMapCreator() {}

ReclaimMapPtr SubReclaimMapCreator::CreateSubReclaimMap(ReclaimMap* mainReclaimMap,
                                                        const index_base::SegmentMergeInfos& mainSegMergeInfos,
                                                        const index_base::SegmentMergeInfos& segMergeInfos,
                                                        const SegmentDirectoryBasePtr& segDir,
                                                        const DeletionMapReaderPtr& delMapReader) const
{
    if (!mainReclaimMap->HasReverseDocIdArray()) {
        mainReclaimMap->InitReverseDocIdArray(mainSegMergeInfos);
    }
    CheckMainAndSubMergeInfos(mainSegMergeInfos, segMergeInfos);

    vector<int32_t> segIdxMap = InitSegmentIdxs(mainSegMergeInfos);
    vector<SegmentReaderPair> mainJoinAttrSegReaders =
        InitJoinAttributeSegmentReaders(mainSegMergeInfos, segDir, MultiTargetSegment(mainReclaimMap));

    return ReclaimJoinedSubDocs(segMergeInfos, mainReclaimMap, segIdxMap, mainJoinAttrSegReaders, delMapReader);
}

ReclaimMapPtr SubReclaimMapCreator::ReclaimJoinedSubDocs(const SegmentMergeInfos& subSegMergeInfos,
                                                         ReclaimMap* mainReclaimMap, const vector<int>& segIdxMap,
                                                         vector<SegmentReaderPair>& mainJoinAttrSegReaders,
                                                         const DeletionMapReaderPtr& subDeletionMapReader) const
{
    uint32_t maxDocCount = subSegMergeInfos.rbegin()->baseDocId + subSegMergeInfos.rbegin()->segmentInfo.docCount;
    uint32_t mergedDocCount = 0;
    for (size_t i = 0; i < subSegMergeInfos.size(); ++i) {
        mergedDocCount += subSegMergeInfos[i].segmentInfo.docCount;
    }
    SubReclaimMapCreator::State state(maxDocCount, mergedDocCount, mainReclaimMap, segIdxMap, mainJoinAttrSegReaders);
    uint32_t newMainDocCount = mainReclaimMap->GetNewDocCount();

    for (docid_t newDocId = 0; newDocId < (docid_t)newMainDocCount; newDocId++) {
        segmentid_t segId;
        docid_t localMainDocId = mainReclaimMap->GetOldDocIdAndSegId(newDocId, segId);
        docid_t baseDocId = subSegMergeInfos[segIdxMap[segId]].baseDocId;
        state.ReclaimOneDoc(segId, baseDocId, localMainDocId, newDocId, subDeletionMapReader);
    }
    return state.ConstructSubReclaimMap(subSegMergeInfos, NeedReverseMapping());
}

vector<int32_t> SubReclaimMapCreator::InitSegmentIdxs(const SegmentMergeInfos& mainSegMergeInfos) const
{
    vector<int32_t> segIdxs;
    for (size_t i = 0; i < mainSegMergeInfos.size(); ++i) {
        segmentid_t segId = mainSegMergeInfos[i].segmentId;
        if ((size_t)segId >= segIdxs.size()) {
            segIdxs.resize(segId + 1, -1);
        }
        segIdxs[segId] = (int32_t)i;
    }
    return segIdxs;
}

void SubReclaimMapCreator::CheckMainAndSubMergeInfos(const SegmentMergeInfos& mainSegMergeInfos,
                                                     const SegmentMergeInfos& subSegMergeInfos) const
{
    if (mainSegMergeInfos.size() != subSegMergeInfos.size()) {
        INDEXLIB_FATAL_ERROR(InconsistentState,
                             "segment size of mainSegMergeInfo and subSegMergeInfo not match [%lu:%lu]",
                             mainSegMergeInfos.size(), subSegMergeInfos.size());
    }
    for (size_t i = 0; i < mainSegMergeInfos.size(); ++i) {
        if (mainSegMergeInfos[i].segmentId != subSegMergeInfos[i].segmentId) {
            INDEXLIB_FATAL_ERROR(InconsistentState,
                                 "segmentId [idx:%lu] of mainSegMergeInfo and subSegMergeInfo not match [%d:%d]", i,
                                 mainSegMergeInfos[i].segmentId, subSegMergeInfos[i].segmentId);
        }
    }
}

vector<SubReclaimMapCreator::SegmentReaderPair>
SubReclaimMapCreator::InitJoinAttributeSegmentReaders(const index_base::SegmentMergeInfos& mainSegMergeInfos,
                                                      const SegmentDirectoryBasePtr& segDir,
                                                      bool multiTargetSegment) const
{
    FieldType fieldType = mMainJoinAttrConfig->GetFieldType();
    if (fieldType != ft_int32) {
        assert(false);
        IE_LOG(ERROR, "unexpected field type [%d], not 3", fieldType);
        return {};
    }

    vector<SegmentReaderPair> attrSegReaders;
    attrSegReaders.resize(mainSegMergeInfos.size());
    for (size_t i = 0; i < mainSegMergeInfos.size(); ++i) {
        const SegmentMergeInfo& segMergeInfo = mainSegMergeInfos[i];
        if (segMergeInfo.segmentInfo.docCount == 0) {
            continue;
        }
        // TODO: legacy for unitttest
        SegmentReaderPtr segmentReader;
        if (segMergeInfo.TEST_inMemorySegmentReader) {
            string attrName = mMainJoinAttrConfig->GetAttrName();
            segmentReader = DYNAMIC_POINTER_CAST(
                SegmentReader, segMergeInfo.TEST_inMemorySegmentReader->GetAttributeSegmentReader(attrName));
        } else {
            segmentReader.reset(new SegmentReader(mMainJoinAttrConfig));
            auto segData = segDir->GetPartitionData()->GetSegmentData(segMergeInfo.segmentId);
            auto attrDir = segData.GetAttributeDirectory(mMainJoinAttrConfig->GetAttrName(), true);
            segmentReader->Open(segData, PatchApplyOption::NoPatch(), attrDir, true);
        }
        // TODO (yiping.typ) maybe need pool
        attrSegReaders[i] = make_pair(segmentReader, segmentReader->CreateReadContext(nullptr));
    }
    return attrSegReaders;
}
}} // namespace indexlib::index
