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

#include <memory>

#include "autil/mem_pool/Pool.h"
#include "indexlib/common/dump_thread_pool.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/util/segment_base_doc_id_info.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

template <typename ModifierPtr>
class SegmentModifierContainer
{
private:
    typedef std::vector<SegmentBaseDocIdInfo> SegBaseDocIdInfoVect;
    typedef std::map<segmentid_t, ModifierPtr> BuiltSegmentModifierMap;
    using SegmentModifierCreateFunction = std::function<ModifierPtr(segmentid_t)>;

public:
    SegmentModifierContainer() {}
    ~SegmentModifierContainer() {}

public:
    void Init(const index_base::PartitionDataPtr& partitionData, SegmentModifierCreateFunction segModiferCreate);
    bool IsDirty() const { return !_segId2BuiltModifierMap.empty(); }
    void Dump(const file_system::DirectoryPtr& dir, segmentid_t segmentId, uint32_t dumpThreadNum,
              const std::string& threadPoolName);

    void TEST_SetSegmentModifierCreateFunction(SegmentModifierCreateFunction segModiferCreate)
    {
        _segmentModifierCreateFn = std::move(segModiferCreate);
    }

    typename BuiltSegmentModifierMap::const_iterator begin() const { return _segId2BuiltModifierMap.begin(); }
    typename BuiltSegmentModifierMap::const_iterator end() const { return _segId2BuiltModifierMap.end(); }

public:
    void GetPatchedSegmentIds(std::vector<segmentid_t>* patchSegIds) const;
    const ModifierPtr& GetSegmentModifier(docid_t globalId, docid_t& localId);
    const SegmentBaseDocIdInfo& GetSegmentBaseInfo(docid_t globalId) const;

private:
    BuiltSegmentModifierMap _segId2BuiltModifierMap;
    SegBaseDocIdInfoVect _dumpedSegmentBaseIdVect;
    SegmentModifierCreateFunction _segmentModifierCreateFn;

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP_TEMPLATE(index, SegmentModifierContainer);

template <typename ModifierPtr>
inline void SegmentModifierContainer<ModifierPtr>::Init(const index_base::PartitionDataPtr& partitionData,
                                                        SegmentModifierCreateFunction segModiferCreate)
{
    _segmentModifierCreateFn = std::move(segModiferCreate);
    auto iter = partitionData->Begin();
    docid_t docCount = 0;
    for (; iter != partitionData->End(); iter++) {
        const auto& segmentData = *iter;
        segmentid_t segmentId = segmentData.GetSegmentId();
        docid_t baseDocId = segmentData.GetBaseDocId();
        docCount += segmentData.GetSegmentInfo()->docCount;
        _dumpedSegmentBaseIdVect.push_back(SegmentBaseDocIdInfo(segmentId, baseDocId));
    }
}

template <typename ModifierPtr>
inline void SegmentModifierContainer<ModifierPtr>::GetPatchedSegmentIds(std::vector<segmentid_t>* patchSegIds) const
{
    auto it = _segId2BuiltModifierMap.begin();
    for (; it != _segId2BuiltModifierMap.end(); ++it) {
        patchSegIds->push_back(it->first);
    }
}

template <typename ModifierPtr>
inline const SegmentBaseDocIdInfo& SegmentModifierContainer<ModifierPtr>::GetSegmentBaseInfo(docid_t globalId) const
{
    size_t i = 1;
    for (; i < _dumpedSegmentBaseIdVect.size(); ++i) {
        if (globalId < _dumpedSegmentBaseIdVect[i].baseDocId) {
            break;
        }
    }
    i--;
    return _dumpedSegmentBaseIdVect[i];
}

template <typename ModifierPtr>
inline const ModifierPtr& SegmentModifierContainer<ModifierPtr>::GetSegmentModifier(docid_t globalId, docid_t& localId)
{
    const SegmentBaseDocIdInfo& segBaseIdInfo = GetSegmentBaseInfo(globalId);
    localId = globalId - segBaseIdInfo.baseDocId;
    segmentid_t segId = segBaseIdInfo.segId;

    auto iter = _segId2BuiltModifierMap.find(segId);
    if (iter == _segId2BuiltModifierMap.end()) {
        _segId2BuiltModifierMap[segId] = _segmentModifierCreateFn(segId);
    }
    return _segId2BuiltModifierMap[segId];
}

template <typename ModifierPtr>
void SegmentModifierContainer<ModifierPtr>::Dump(const file_system::DirectoryPtr& dir, segmentid_t segmentId,
                                                 uint32_t dumpThreadNum, const std::string& threadPoolName)
{
    auto it = begin();
    if (dumpThreadNum > 1) {
        IE_LOG(INFO, "MultiThreadDumpPathFile with dump thred num [%u] begin!", dumpThreadNum);
        common::DumpThreadPool dumpThreadPool(dumpThreadNum, 1024);
        if (!dumpThreadPool.start(threadPoolName)) {
            INDEXLIB_THROW(util::RuntimeException, "create dump thread pool failed");
        }
        std::vector<std::unique_ptr<common::DumpItem>> dumpItems;
        for (; it != end(); ++it) {
            it->second->CreateDumpItems(dir, segmentId, &dumpItems);
        }
        for (size_t i = 0; i < dumpItems.size(); ++i) {
            dumpThreadPool.pushWorkItem(dumpItems[i].release());
        }
        dumpThreadPool.stop();
        IE_LOG(INFO, "MultiThreadDumpPathFile end, dumpMemoryPeak [%lu]", dumpThreadPool.getPeakOfMemoryUse());
    } else {
        for (; it != end(); ++it) {
            it->second->Dump(dir, segmentId);
        }
    }
}
}} // namespace indexlib::index
