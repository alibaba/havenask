#ifndef __INDEXLIB_RECLAIM_MAP_H
#define __INDEXLIB_RECLAIM_MAP_H

#include <tr1/memory>
#include <functional>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/slice_array/aligned_slice_array.h"
#include "indexlib/util/vector_typed.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"
#include "indexlib/index_base/segment_mapper.h"
#include <algorithm>

DECLARE_REFERENCE_CLASS(index, SegmentDirectoryBase);
DECLARE_REFERENCE_CLASS(index, DeletionMapReader);
DECLARE_REFERENCE_CLASS(index, AttributeSegmentReader);
DECLARE_REFERENCE_CLASS(index, DocInfoAllocator);
DECLARE_REFERENCE_CLASS(index, MultiComparator);
DECLARE_REFERENCE_CLASS(index, MultiAttributeEvaluator);
DECLARE_REFERENCE_CLASS(index, OfflineAttributeSegmentReaderContainer);
DECLARE_REFERENCE_CLASS(file_system, BufferedFileReader);
DECLARE_REFERENCE_CLASS(file_system, BufferedFileWriter);
DECLARE_REFERENCE_CLASS(file_system, FileWriter);
DECLARE_REFERENCE_CLASS(file_system, FileReader);
DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(config, AttributeConfig);

IE_NAMESPACE_BEGIN(index);

class ReclaimMap
{
public:
    static const std::string RECLAIM_MAP_FILE_NAME;
public:
    typedef util::AlignedSliceArray<docid_t>  AlignedSliceArrayType;
    typedef std::tr1::shared_ptr<AlignedSliceArrayType> AlignedSliceArrayTypePtr;

    typedef std::pair<segmentid_t, docid_t> SegBaseDocId;

    typedef std::vector<config::AttributeConfigPtr> AttributeConfigVector;
    typedef std::vector<SortPattern> SortPatternVec;

public:
    ReclaimMap();
    ReclaimMap(const ReclaimMap& other);
    virtual ~ReclaimMap();

public:
    virtual void Init(const index_base::SegmentMergeInfos& segMergeInfos,
                      const DeletionMapReaderPtr& deletionMapReader,
                      const OfflineAttributeSegmentReaderContainerPtr& attrReaders,
                      bool needReverseMapping);

    virtual void Init(const index_base::SegmentMergeInfos& segMergeInfos, 
                      const DeletionMapReaderPtr& deletionMapReader,
                      const OfflineAttributeSegmentReaderContainerPtr& attrReaders,
                      const AttributeConfigVector& attributeConfigs,
                      const SortPatternVec& sortPatterns,
                      const SegmentDirectoryBasePtr& segDir,
                      bool needReverseMapping);

    // for sub parition
    virtual void Init(ReclaimMap* mainReclaimMap,
                      const SegmentDirectoryBasePtr& mainSegmentDirectory,
                      const config::AttributeConfigPtr& mainJoinAttrConfig,
                      const index_base::SegmentMergeInfos& mainSegMergeInfos,
                      const index_base::SegmentMergeInfos& subSegMergeInfos,
                      const DeletionMapReaderPtr& subDeletionMapReader,
                      bool needReverseMapping);

    void SetSegmentSplitHandler(std::function<segmentindex_t(segmentid_t, docid_t)> fn)
    {
        mSegmentSplitHandler = std::move(fn);
    }

    // return docid in plan
    docid_t GetNewId(docid_t oldId) const
    {
        assert(oldId < (int64_t)mDocIdArray.size());
        return mDocIdArray[oldId];
    }

    segmentindex_t GetTargetSegmentIndex(docid_t newId) const
    {
        if (mTargetBaseDocIds.empty())
        {
            return 0;
        }
        auto iter
            = std::upper_bound(mTargetBaseDocIds.begin(), mTargetBaseDocIds.end(), newId) - 1;
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
        if (mTargetBaseDocIds.empty())
        {
            return 0;
        }
        assert(segIdx < mTargetBaseDocIds.size());
        return mTargetBaseDocIds[segIdx];
    }

    std::pair<segmentindex_t, docid_t> GetLocalId(docid_t newId) const
    {
        if (newId == INVALID_DOCID)
        {
            return { 0, INVALID_DOCID };
        }
        auto segIdx = GetTargetSegmentIndex(newId);
        if (segIdx == 0) {
            return { 0, newId };
        }
        return { segIdx, newId - mTargetBaseDocIds[segIdx] };
    }

    std::pair<segmentindex_t, docid_t> GetNewLocalId(docid_t oldId) const
    {
        if (oldId == INVALID_DOCID)
        {
            return { 0, INVALID_DOCID };
        }
        auto newId = GetNewId(oldId);
        return GetLocalId(newId);
    }

    size_t GetTargetSegmentDocCount(segmentindex_t segmentIdx) const
    {
        if (mTargetBaseDocIds.empty())
        {
            return GetNewDocCount();
        }
        assert(segmentIdx < mTargetBaseDocIds.size());
        size_t next = 0;
        if (segmentIdx == mTargetBaseDocIds.size() - 1)
        {
            next = GetNewDocCount();
        }
        else
        {
            next = mTargetBaseDocIds[segmentIdx + 1];
        }
        return next - static_cast<size_t>(mTargetBaseDocIds[segmentIdx]);
    }

    docid_t GetOldDocIdAndSegId(docid_t newDocId, segmentid_t &segId) const;

    uint32_t GetDeletedDocCount() const
    { return mDeletedDocCount; }

    uint32_t GetTotalDocCount() const
    { return mDocIdArray.size();}

    //virtual for test
    virtual uint32_t GetNewDocCount() const 
    { return mNewDocId; }

    ReclaimMap* Clone() const;

    void InitReverseDocIdArray(const index_base::SegmentMergeInfos& segMergeInfos);

    void StoreForMerge(const std::string &rootPath,
                       const index_base::SegmentMergeInfos& segMergeInfos) const;
    void StoreForMerge(const file_system::DirectoryPtr &directory,
                       const std::string &fileName,
                       const index_base::SegmentMergeInfos& segMergeInfos) const;

    bool LoadForMerge(const std::string& rootPath,
                      const index_base::SegmentMergeInfos& segMergeInfos,
                      int64_t mergeMetaBinaryVersion);
    bool LoadForMerge(const file_system::DirectoryPtr& directory,
                      const std::string& fileName,
                      const index_base::SegmentMergeInfos& segMergeInfos,
                      int64_t mergeMetaBinaryVersion);

    const std::vector<docid_t>& GetJoinValueArray() const
    {
        return mJoinValueArray;
    }

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

    const std::vector<docid_t> &GetSliceArray() const
    {
        return mDocIdArray;
    }

public:
    // for test
    void Init(uint32_t docCount);
    void SetNewId(docid_t oldId, docid_t newId);
    void TEST_SetTargetBaseDocIds(std::vector<docid_t> baseDocIds)
    {
        mTargetBaseDocIds = std::move(baseDocIds);
    }
    const std::vector<docid_t>& TEST_GetTargetBaseDocIds() const { return mTargetBaseDocIds; }

public:
    // for test
    void SetSliceArray(const std::vector<docid_t> &docIdArray)
    {
        mDocIdArray = docIdArray;
    }

    bool LoadDocIdArray(file_system::FileReaderPtr& reader,
                        const index_base::SegmentMergeInfos& segMergeInfos,
                        int64_t mergeMetaBinaryVersion);

private:
    void ReclaimOneSegment(segmentid_t segId,
                           docid_t baseDocId, uint32_t docCount, 
                           const DeletionMapReaderPtr& deletionMapReader,
                           index_base::SegmentMapper& segMapper);

    void SortByWeightInit(const index_base::SegmentMergeInfos& segMergeInfos, 
                          const DeletionMapReaderPtr& deletionMapReader,
                          const AttributeConfigVector& attributeConfigs,
                          const SortPatternVec& sortPatterns,
                          const SegmentDirectoryBasePtr& segDir);

    bool NeedSplitSegment() const { return mSegmentSplitHandler.operator bool(); }

    void InitMultiComparator(
            const AttributeConfigVector& attributeConfigs, 
            const SortPatternVec& sortPatterns,
            MultiComparatorPtr comparator,
            DocInfoAllocatorPtr docInfoAllocator);

    void InitMultiAttrEvaluator(
            const index_base::SegmentMergeInfo& segMergeInfo,
            const AttributeConfigVector& attributeConfigs,
            const SegmentDirectoryBasePtr& segDir, 
            DocInfoAllocatorPtr docInfoAllocator,
            MultiAttributeEvaluatorPtr multiEvaluatorPtr);

    void ReclaimOneDoc(segmentid_t segId, docid_t baseDocId, docid_t localId, 
                       const DeletionMapReaderPtr& deletionMapReader,
                       index_base::SegmentMapper& segMapper);
    
    void CheckMainAndSubMergeInfos(const index_base::SegmentMergeInfos& mainSegMergeInfos, 
                                   const index_base::SegmentMergeInfos& subSegMergeInfos);

    void InitSegmentIdxs(std::vector<int32_t>& segIdxs,
                         const index_base::SegmentMergeInfos& mainSegMergeInfos);

    void InitJoinAttributeSegmentReaders(std::vector<AttributeSegmentReaderPtr>& attrSegReaders,
        const SegmentDirectoryBasePtr& mainSegmentDirectory,
        const config::AttributeConfigPtr& mainJoinAttrConfig,
        const index_base::SegmentMergeInfos& mainSegMergeInfos, bool multiTargetSegment);

    void GetMainJoinValues(const AttributeSegmentReaderPtr& segReader,
                           docid_t localDocId, docid_t& lastDocJoinValue,
                           docid_t& curDocJoinValue);

    void ReclaimJoinedSubDocs(ReclaimMap* mainReclaimMap, 
                              const index_base::SegmentMergeInfos& subSegMergeInfos, 
                              const std::vector<int>& segIdxMap, 
                              const std::vector<AttributeSegmentReaderPtr>& mainJoinAttrSegReaders, 
                              const DeletionMapReaderPtr& subDeletionMapReader);

    void MakeTargetBaseDocIds(const index_base::SegmentMapper &segMapper,
                              segmentindex_t maxSegIdx);
    void RewriteDocIdArray(const index_base::SegmentMergeInfos& segMergeInfos,
        const index_base::SegmentMapper& segMapper);

    void InnerStore(const file_system::FileWriterPtr &writer,
                     const index_base::SegmentMergeInfos& segMergeInfos) const;
    bool InnerLoad(file_system::FileReaderPtr &reader,
                   const index_base::SegmentMergeInfos& segMergeInfos,
                   int64_t mergeMetaBinaryVersion);

    template <typename T, typename SizeType = uint32_t>
    bool LoadVector(file_system::FileReaderPtr& reader, std::vector<T>& vec);
    template <typename T, typename SizeType = uint32_t>
    void StoreVector(const file_system::FileWriterPtr& writer, const std::vector<T>& vec) const;

protected:
    typedef std::pair<segmentid_t, docid_t> GlobalId;
    typedef util::VectorTyped<GlobalId> GlobalIdVector;
    typedef std::tr1::shared_ptr<GlobalIdVector> GlobalIdVectorPtr;

protected:
    std::vector<docid_t> mDocIdArray;
    std::vector<docid_t> mJoinValueArray;

    GlobalIdVectorPtr mReverseGlobalIdArray; // newDocId --> old global id 
    uint32_t mDeletedDocCount;
    docid_t mNewDocId;
    OfflineAttributeSegmentReaderContainerPtr mAttrReaderContainer;
    std::function<segmentindex_t(segmentid_t, docid_t)> mSegmentSplitHandler;

    std::vector<docid_t> mTargetBaseDocIds;    
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ReclaimMap);

//////////////////////////////////////////
inline void ReclaimMap::SetNewId(docid_t oldId, docid_t newId)
{
    mDocIdArray[oldId] = newId;
    if (newId == INVALID_DOCID)
    {
        ++mDeletedDocCount;
    }
}

inline docid_t ReclaimMap::GetOldDocIdAndSegId(docid_t newDocId, 
        segmentid_t &segId) const
{
    assert(newDocId < mNewDocId);
    assert(mReverseGlobalIdArray);
    GlobalId gid = (*mReverseGlobalIdArray)[newDocId];
    segId = gid.first;
    return gid.second;
}



IE_NAMESPACE_END(index);

#endif //__INDEXLIB_RECLAIM_MAP_H
