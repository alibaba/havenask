#ifndef __INDEXLIB_MERGE_PLAN_H
#define __INDEXLIB_MERGE_PLAN_H

#include <tr1/memory>
#include <vector>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"
#include "indexlib/index_base/index_meta/segment_topology_info.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/merger/merge_define.h"
#include <autil/legacy/jsonizable.h>

DECLARE_REFERENCE_CLASS(file_system, Directory);

IE_NAMESPACE_BEGIN(merger);

class MergePlan : public autil::legacy::Jsonizable
{
private:
    struct SegmentDesc : public autil::legacy::Jsonizable
    {
        segmentid_t segmentId = INVALID_SEGMENTID;
        index_base::SegmentTopologyInfo topologyInfo;
        index_base::SegmentInfo segmentInfo;
        index_base::SegmentInfo subSegmentInfo;
        autil::legacy::json::JsonMap segmentCustomizeMetrics;

        SegmentDesc() = default;
        void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    };

public :
    typedef std::set<segmentid_t> SegmentSet;
    typedef std::vector<SegmentDesc> TargetSegmentDescs;

    static const std::string MERGE_PLAN_FILE_NAME;
public:
    MergePlan();
    ~MergePlan();
    
public:
    class Iterator
    {
    public:
        Iterator(const SegmentSet& segments)
            : mSegments(segments)
            , mIter(mSegments.begin())
        {
        }
        
        Iterator(const Iterator& iter)
            : mSegments(iter.mSegments)
            , mIter(iter.mSegments.begin())
        {
        }
        
        bool HasNext() const { return (mIter != mSegments.end()); }
        
        segmentid_t Next() { return (*mIter++); }
        
    private:
        const SegmentSet& mSegments;
        SegmentSet::const_iterator mIter;
    };

public:
    void AddSegment(const index_base::SegmentMergeInfo& segMergeInfo);

    size_t GetSegmentCount() const
    {
        return mSegments.size();
    }

    bool HasSegment(segmentid_t segIdx) const
    {
        return (mSegments.find(segIdx) != mSegments.end());
    }

    Iterator CreateIterator() const
    {
        return Iterator(mSegments);
    }
    
    size_t GetTargetSegmentCount() const { return mTargetSegmentDescs.size(); }
    
    void AddTargetSegment() { mTargetSegmentDescs.push_back(SegmentDesc()); }
    
    segmentid_t GetTargetSegmentId(size_t idx) const
    {
        assert(idx < mTargetSegmentDescs.size());
        return mTargetSegmentDescs[idx].segmentId;
    }

    void SetTargetSegmentId(size_t idx, segmentid_t targetSegmentId)
    {
        assert(idx < mTargetSegmentDescs.size());
        mTargetSegmentDescs[idx].segmentId = targetSegmentId;
    }
    
    const index_base::SegmentTopologyInfo& GetTargetTopologyInfo(size_t idx) const
    {
        assert(idx < mTargetSegmentDescs.size());
        return mTargetSegmentDescs[idx].topologyInfo;
    }
    
    void SetTargetTopologyInfo(size_t idx, const index_base::SegmentTopologyInfo& segTopoInfo)
    {
        assert(idx < mTargetSegmentDescs.size());
        mTargetSegmentDescs[idx].topologyInfo = segTopoInfo;
    }
    
    const index_base::SegmentInfo& GetTargetSegmentInfo(size_t idx) const
    {
        assert(idx < mTargetSegmentDescs.size());
        return mTargetSegmentDescs[idx].segmentInfo;
    }
    index_base::SegmentInfo& GetTargetSegmentInfo(size_t idx)
    {
        assert(idx < mTargetSegmentDescs.size());
        return mTargetSegmentDescs[idx].segmentInfo;
    }    

    void SetTargetSegmentInfo(size_t idx, const index_base::SegmentInfo& segmentInfo) {
        assert(idx < mTargetSegmentDescs.size());
        mTargetSegmentDescs[idx].segmentInfo = segmentInfo;
    }

    const index_base::SegmentInfo& GetSubTargetSegmentInfo(size_t idx) const
    {
        assert(idx < mTargetSegmentDescs.size());
        return mTargetSegmentDescs[idx].subSegmentInfo;
    }
    
    index_base::SegmentInfo& GetSubTargetSegmentInfo(size_t idx)
    {
        assert(idx < mTargetSegmentDescs.size());
        return mTargetSegmentDescs[idx].subSegmentInfo;
    }    

    void SetSubTargetSegmentInfo(size_t idx, const index_base::SegmentInfo& segmentInfo) {
        assert(idx < mTargetSegmentDescs.size());
        mTargetSegmentDescs[idx].subSegmentInfo = segmentInfo;
    }

    const autil::legacy::json::JsonMap& GetTargetSegmentCustomizeMetrics(size_t idx) const
    {
        assert(idx < mTargetSegmentDescs.size());
        return mTargetSegmentDescs[idx].segmentCustomizeMetrics;
    }

    void SetTargetSegmentCustomizeMetrics(size_t idx, autil::legacy::json::JsonMap jsonMap) {
        assert(idx < mTargetSegmentDescs.size());
        mTargetSegmentDescs[idx].segmentCustomizeMetrics = std::move(jsonMap);
    }

    const index_base::SegmentMergeInfos& GetSegmentMergeInfos() const
    { return mSegmentMergeInfos; }

    void SetSubSegmentMergeInfos(const index_base::SegmentMergeInfos& subSegMergeInfos)
    {
        mSubSegmentMergeInfos = subSegMergeInfos;
    }
    const index_base::SegmentMergeInfos& GetSubSegmentMergeInfos() const
    {
        return mSubSegmentMergeInfos;
    }

    bool IsEntireDataSet(const index_base::Version& targetVersion) const
    {
        if (targetVersion.GetSegmentCount() != GetTargetSegmentCount())
        {
            return false;
        }
        for (size_t i = 0; i < GetTargetSegmentCount(); ++i)
        {
            if (!targetVersion.HasSegment(GetTargetSegmentId(i)))
            {
                return false;
            }
        }
        return true;
    }

    std::string ToString() const
    {
        std::stringstream ss;
        SegmentSet::const_iterator iter = mSegments.begin();
        while(iter != mSegments.end())
        {
            if (iter != mSegments.begin())
            {
                ss << ", ";
            }
            ss << *iter;
            iter++;
        }
        return ss.str();
    }

    void Store(const std::string& rootPath) const;
    void Store(const file_system::DirectoryPtr& directory) const;
    bool Load(const std::string& rootPath, int64_t mergeMetaVersionFile = define::INDEX_MERGE_META_BINARY_VERSION);
    bool Load(const file_system::DirectoryPtr& directory,
              int64_t mergeMetaVersionFile = define::INDEX_MERGE_META_BINARY_VERSION);
    
    bool Empty() const
    {
        return mSegments.empty();
    }
    
    void Clear()
    {
        mSegments.clear();
        mTargetSegmentDescs.clear();
        mSegmentMergeInfos.clear();
        mSubSegmentMergeInfos.clear();
    }

    void ClearTargetSegments() {
        mTargetSegmentDescs.clear();        
    }

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

    void TEST_SetSegmentMergeInfos(index_base::SegmentMergeInfos segMergeInfos)
    {
        mSegmentMergeInfos = std::move(segMergeInfos);
    }
    void TEST_SetSubSegmentMergeInfos(index_base::SegmentMergeInfos subSegMergeInfos)
    {
        mSubSegmentMergeInfos = std::move(subSegMergeInfos);
    }    

private:
    void LoadLegacyMergePlan(const std::string& content);
    
private:
    static const std::string LEGACY_MERGE_PLAN_META_FILE_NAME;

private:
    friend class MergeTask;
    SegmentSet mSegments;
    TargetSegmentDescs mTargetSegmentDescs;
    index_base::SegmentMergeInfos mSegmentMergeInfos;
    index_base::SegmentMergeInfos mSubSegmentMergeInfos;
};

DEFINE_SHARED_PTR(MergePlan);

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_MERGE_PLAN_H
