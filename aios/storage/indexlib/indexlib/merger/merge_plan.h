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
#ifndef __INDEXLIB_MERGE_PLAN_H
#define __INDEXLIB_MERGE_PLAN_H

#include <memory>
#include <vector>

#include "autil/legacy/jsonizable.h"
#include "indexlib/common_define.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"
#include "indexlib/index_base/index_meta/segment_temperature_meta.h"
#include "indexlib/index_base/index_meta/segment_topology_info.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/merger/merge_define.h"

DECLARE_REFERENCE_CLASS(file_system, Directory);

namespace indexlib { namespace merger {

class MergePlan : public autil::legacy::Jsonizable
{
private:
    struct SegmentDesc : public autil::legacy::Jsonizable {
        segmentid_t segmentId = INVALID_SEGMENTID;
        index_base::SegmentTopologyInfo topologyInfo;
        index_base::SegmentInfo segmentInfo;
        index_base::SegmentInfo subSegmentInfo;
        autil::legacy::json::JsonMap segmentCustomizeMetrics;
        index_base::SegmentTemperatureMeta temperatureMeta;

        SegmentDesc() = default;
        void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    };

public:
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
        Iterator(const SegmentSet& segments) : mSegments(segments), mIter(mSegments.begin()) {}

        Iterator(const Iterator& iter) : mSegments(iter.mSegments), mIter(iter.mSegments.begin()) {}

        bool HasNext() const { return (mIter != mSegments.end()); }

        segmentid_t Next() { return (*mIter++); }

    private:
        const SegmentSet& mSegments;
        SegmentSet::const_iterator mIter;
    };

public:
    void AddSegment(const index_base::SegmentMergeInfo& segMergeInfo);

    size_t GetSegmentCount() const { return mSegments.size(); }

    bool HasSegment(segmentid_t segIdx) const { return (mSegments.find(segIdx) != mSegments.end()); }

    Iterator CreateIterator() const { return Iterator(mSegments); }

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
        mTargetSegmentDescs[idx].temperatureMeta.segmentId = targetSegmentId;
    }

    const index_base::SegmentTemperatureMeta& GetTargetSegmentTemperatureMeta(size_t idx) const
    {
        assert(idx < mTargetSegmentDescs.size());
        return mTargetSegmentDescs[idx].temperatureMeta;
    }

    void SetTargetSegmentTemperatureMeta(size_t idx, const index_base::SegmentTemperatureMeta& meta)
    {
        assert(idx < mTargetSegmentDescs.size());
        mTargetSegmentDescs[idx].temperatureMeta = meta;
        mTargetSegmentDescs[idx].segmentInfo.AddDescription(SEGMENT_CUSTOMIZE_METRICS_GROUP,
                                                            ToJsonString(meta.GenerateCustomizeMetric()));
    }

    void SetTargetSegmentCompressFingerPrint(size_t idx, uint64_t fingerPrint)
    {
        assert(idx < mTargetSegmentDescs.size());
        mTargetSegmentDescs[idx].segmentInfo.AddDescription(SEGMENT_COMPRESS_FINGER_PRINT,
                                                            autil::StringUtil::toString(fingerPrint));
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

    void SetTargetSegmentInfo(size_t idx, const index_base::SegmentInfo& segmentInfo)
    {
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

    void SetSubTargetSegmentInfo(size_t idx, const index_base::SegmentInfo& segmentInfo)
    {
        assert(idx < mTargetSegmentDescs.size());
        mTargetSegmentDescs[idx].subSegmentInfo = segmentInfo;
    }

    const autil::legacy::json::JsonMap& GetTargetSegmentCustomizeMetrics(size_t idx) const
    {
        assert(idx < mTargetSegmentDescs.size());
        return mTargetSegmentDescs[idx].segmentCustomizeMetrics;
    }

    void SetTargetSegmentCustomizeMetrics(size_t idx, autil::legacy::json::JsonMap jsonMap)
    {
        assert(idx < mTargetSegmentDescs.size());
        mTargetSegmentDescs[idx].segmentCustomizeMetrics = std::move(jsonMap);
    }

    const index_base::SegmentMergeInfos& GetSegmentMergeInfos() const { return mSegmentMergeInfos; }

    void SetSubSegmentMergeInfos(const index_base::SegmentMergeInfos& subSegMergeInfos)
    {
        mSubSegmentMergeInfos = subSegMergeInfos;
    }
    const index_base::SegmentMergeInfos& GetSubSegmentMergeInfos() const { return mSubSegmentMergeInfos; }

    bool IsEntireDataSet(const index_base::Version& targetVersion) const
    {
        if (targetVersion.GetSegmentCount() != GetTargetSegmentCount()) {
            return false;
        }
        for (size_t i = 0; i < GetTargetSegmentCount(); ++i) {
            if (!targetVersion.HasSegment(GetTargetSegmentId(i))) {
                return false;
            }
        }
        return true;
    }

    std::string ToString() const
    {
        std::string ss;
        SegmentSet::const_iterator iter = mSegments.begin();
        while (iter != mSegments.end()) {
            if (iter != mSegments.begin()) {
                ss += ",";
            }
            ss += autil::StringUtil::toString(*iter);
            iter++;
        }
        return ss;
    }

    std::string TargetSegmentToString() const
    {
        std::string ss;
        for (size_t i = 0; i < mTargetSegmentDescs.size(); i++) {
            if (i != 0) {
                ss += ",";
            }
            ss += autil::StringUtil::toString(GetTargetSegmentId(i));
        }
        return ss;
    }

    void Store(const file_system::DirectoryPtr& directory) const;
    bool Load(const file_system::DirectoryPtr& directory,
              int64_t mergeMetaVersionFile = define::INDEX_MERGE_META_BINARY_VERSION);

    bool Empty() const { return mSegments.empty(); }

    void Clear()
    {
        mSegments.clear();
        mTargetSegmentDescs.clear();
        mSegmentMergeInfos.clear();
        mSubSegmentMergeInfos.clear();
    }

    void ClearTargetSegments() { mTargetSegmentDescs.clear(); }

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
}} // namespace indexlib::merger

#endif //__INDEXLIB_MERGE_PLAN_H
