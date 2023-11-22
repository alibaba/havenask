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

#include "indexlib/common_define.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/VectorTyped.h"
#include "indexlib/util/slice_array/AlignedSliceArray.h"

DECLARE_REFERENCE_CLASS(file_system, BufferedFileReader);
DECLARE_REFERENCE_CLASS(file_system, BufferedFileWriter);
DECLARE_REFERENCE_CLASS(file_system, FileWriter);
DECLARE_REFERENCE_CLASS(file_system, FileReader);
DECLARE_REFERENCE_CLASS(file_system, Directory);

namespace indexlib { namespace index {

class ReclaimMap
{
protected:
    typedef std::pair<segmentid_t, docid_t> GlobalId;
    typedef util::VectorTyped<GlobalId> GlobalIdVector;
    typedef std::shared_ptr<GlobalIdVector> GlobalIdVectorPtr;
    typedef std::vector<docid_t> DocOrder;
    typedef std::shared_ptr<DocOrder> DocOrderPtr;

public:
    static const std::string RECLAIM_MAP_FILE_NAME;

public:
    ReclaimMap();
    ReclaimMap(uint32_t deletedDocCount, docid_t newDocId, std::vector<docid_t>&& docIdArray,
               std::vector<docid_t>&& joinValueArray, std::vector<docid_t>&& targetBaseDocIds);
    ReclaimMap(const ReclaimMap& other);
    virtual ~ReclaimMap();

public:
    // return docid in plan
    docid_t GetNewId(docid_t oldId) const
    {
        assert(oldId < (int64_t)mDocIdArray.size());
        return mDocIdArray[oldId];
    }
    docid_t GetDocOrder(docid_t oldId) const
    {
        if (mDocIdOrder) {
            assert(oldId < (int64_t)mDocIdOrder->size());
            return (*mDocIdOrder)[oldId];
        }
        return GetNewId(oldId);
    }

    segmentindex_t GetTargetSegmentIndex(docid_t newId) const
    {
        if (mTargetBaseDocIds.empty()) {
            return 0;
        }
        auto iter = std::upper_bound(mTargetBaseDocIds.begin(), mTargetBaseDocIds.end(), newId) - 1;
        return iter - mTargetBaseDocIds.begin();
    }
    const std::vector<docid_t>& GetTargetBaseDocIds() const { return mTargetBaseDocIds; }
    size_t GetTargetSegmentCount() const
    {
        if (mTargetBaseDocIds.empty()) {
            return 1;
        }
        return mTargetBaseDocIds.size();
    }

    docid_t GetTargetBaseDocId(segmentindex_t segIdx) const
    {
        if (mTargetBaseDocIds.empty()) {
            return 0;
        }
        assert(segIdx < mTargetBaseDocIds.size());
        return mTargetBaseDocIds[segIdx];
    }

    std::pair<segmentindex_t, docid_t> GetLocalId(docid_t newId) const
    {
        if (newId == INVALID_DOCID) {
            return {0, INVALID_DOCID};
        }
        auto segIdx = GetTargetSegmentIndex(newId);
        if (segIdx == 0) {
            return {0, newId};
        }
        return {segIdx, newId - mTargetBaseDocIds[segIdx]};
    }

    std::pair<segmentindex_t, docid_t> GetNewLocalId(docid_t oldId) const
    {
        if (oldId == INVALID_DOCID) {
            return {0, INVALID_DOCID};
        }
        auto newId = GetNewId(oldId);
        return GetLocalId(newId);
    }

    size_t GetTargetSegmentDocCount(segmentindex_t segmentIdx) const
    {
        if (mTargetBaseDocIds.empty()) {
            return GetNewDocCount();
        }
        assert(segmentIdx < mTargetBaseDocIds.size());
        size_t next = 0;
        if (segmentIdx == mTargetBaseDocIds.size() - 1) {
            next = GetNewDocCount();
        } else {
            next = mTargetBaseDocIds[segmentIdx + 1];
        }
        return next - static_cast<size_t>(mTargetBaseDocIds[segmentIdx]);
    }

    docid_t GetOldDocIdAndSegId(docid_t newDocId, segmentid_t& segId) const;

    uint32_t GetDeletedDocCount() const { return mDeletedDocCount; }

    uint32_t GetTotalDocCount() const { return mDocIdArray.size(); }

    // virtual for test
    virtual uint32_t GetNewDocCount() const { return mNewDocId; }

    ReclaimMap* Clone() const;

    bool HasReverseDocIdArray() const { return mReverseGlobalIdArray != NULL; }

    void StoreForMerge(const std::string& rootPath, const index_base::SegmentMergeInfos& segMergeInfos) const;
    void StoreForMerge(const file_system::DirectoryPtr& directory, const std::string& fileName,
                       const index_base::SegmentMergeInfos& segMergeInfos) const;

    bool LoadForMerge(const std::string& rootPath, const index_base::SegmentMergeInfos& segMergeInfos,
                      int64_t mergeMetaBinaryVersion);
    bool LoadForMerge(const file_system::DirectoryPtr& directory, const std::string& fileName,
                      const index_base::SegmentMergeInfos& segMergeInfos, int64_t mergeMetaBinaryVersion);

    const std::vector<docid_t>& GetJoinValueArray() const { return mJoinValueArray; }

    void InitJoinValueArray(std::vector<docid_t>& joinValueArray)
    {
        assert(mJoinValueArray.empty());
        mJoinValueArray.swap(joinValueArray);
    }

    void ReleaseJoinValueArray()
    {
        std::vector<docid_t> tmpEmptyArray;
        mJoinValueArray.swap(tmpEmptyArray);
    }

    int64_t EstimateMemoryUse() const;

    const std::vector<docid_t>& GetSliceArray() const { return mDocIdArray; }

    void InitReverseDocIdArray(const index_base::SegmentMergeInfos& segMergeInfos);

    void SetDocIdOrder(const DocOrderPtr docIdOrder) { mDocIdOrder = std::move(docIdOrder); }
    GlobalIdVectorPtr GetGlobalidVector() const { return mReverseGlobalIdArray; }

public:
    // for test
    void Init(uint32_t docCount);
    void SetNewId(docid_t oldId, docid_t newId);
    void TEST_SetTargetBaseDocIds(std::vector<docid_t> baseDocIds) { mTargetBaseDocIds = std::move(baseDocIds); }
    const std::vector<docid_t>& TEST_GetTargetBaseDocIds() const { return mTargetBaseDocIds; }

public:
    // for test
    void SetSliceArray(const std::vector<docid_t>& docIdArray) { mDocIdArray = docIdArray; }

    bool LoadDocIdArray(file_system::FileReaderPtr& reader, const index_base::SegmentMergeInfos& segMergeInfos,
                        int64_t mergeMetaBinaryVersion);

    void InnerStore(const file_system::FileWriterPtr& writer, const index_base::SegmentMergeInfos& segMergeInfos) const;
    bool InnerLoad(file_system::FileReaderPtr& reader, const index_base::SegmentMergeInfos& segMergeInfos,
                   int64_t mergeMetaBinaryVersion);

    template <typename T, typename SizeType = uint32_t>
    bool LoadVector(file_system::FileReaderPtr& reader, std::vector<T>& vec);
    template <typename T, typename SizeType = uint32_t>
    void StoreVector(const file_system::FileWriterPtr& writer, const std::vector<T>& vec) const;

protected:
    uint32_t mDeletedDocCount;
    //当前reclaimmap中合法的doc总数
    docid_t mNewDocId; // TODO: rename it total_doc_count.
    // oldDocId到newDocId的映射（oldDocId是全局docid，newDocId是plan中的doc顺序）
    std::vector<docid_t> mDocIdArray; // oldDocId --> newDocId
    //主字表docid的映射关系
    std::vector<docid_t> mJoinValueArray; // sub docid map
    //目标segment的base docid
    std::vector<docid_t> mTargetBaseDocIds;
    // sort merge + split的场景,倒排链merge归并的时候,会出现下面的情况
    // posting1(segmentid_localDocid):1_3, 2_2, 3_1
    // posting2:3_3, 1_1
    // posting链1，2归并的时候按照docid由小到大会发生先加1_3，后加1_1的情况
    // mDocIdOrder就是用于解这个问题
    DocOrderPtr mDocIdOrder;                 // old docid order
    GlobalIdVectorPtr mReverseGlobalIdArray; // newDocId --> old global id

    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ReclaimMap);

//////////////////////////////////////////
inline void ReclaimMap::SetNewId(docid_t oldId, docid_t newId)
{
    mDocIdArray[oldId] = newId;
    if (newId == INVALID_DOCID) {
        ++mDeletedDocCount;
    }
}

inline docid_t ReclaimMap::GetOldDocIdAndSegId(docid_t newDocId, segmentid_t& segId) const
{
    assert(newDocId < mNewDocId);
    assert(mReverseGlobalIdArray);
    GlobalId gid = (*mReverseGlobalIdArray)[newDocId];
    segId = gid.first;
    return gid.second;
}
}} // namespace indexlib::index
