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

#include "autil/legacy/jsonizable.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class SegmentUpdateInfo : public autil::legacy::Jsonizable
{
public:
    SegmentUpdateInfo(segmentid_t segId = INVALID_SEGMENTID, uint32_t docCount = 0)
        : updateSegId(segId)
        , updateDocCount(docCount)
    {
    }

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
    {
        json.Jsonize("update_segment_id", updateSegId);
        json.Jsonize("update_doc_count", updateDocCount);
    }

    bool operator==(const SegmentUpdateInfo& other) const
    {
        return updateSegId == other.updateSegId && updateDocCount == other.updateDocCount;
    }

    bool operator!=(const SegmentUpdateInfo& other) const { return !(*this == other); }

public:
    segmentid_t updateSegId;
    uint32_t updateDocCount;
};

class AttributeUpdateInfo : public autil::legacy::Jsonizable
{
public:
    typedef std::vector<SegmentUpdateInfo> SegmentUpdateInfoVec;
    typedef std::map<segmentid_t, SegmentUpdateInfo> SegmentUpdateInfoMap;

public:
    class Iterator
    {
    public:
        Iterator(const SegmentUpdateInfoMap& map) : mCursor(map.begin()), mEndCur(map.end()) {}

        bool HasNext() const { return mCursor != mEndCur; }

        SegmentUpdateInfo Next()
        {
            assert(HasNext());
            SegmentUpdateInfo info = mCursor->second;
            ++mCursor;
            return info;
        }

    private:
        SegmentUpdateInfoMap::const_iterator mCursor;
        SegmentUpdateInfoMap::const_iterator mEndCur;
    };

public:
    AttributeUpdateInfo() {}
    ~AttributeUpdateInfo() {}

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

    void Load(const file_system::DirectoryPtr& directory);

    bool operator==(const AttributeUpdateInfo& other) const;

    bool Add(const SegmentUpdateInfo& segUpdateInfo)
    {
        std::pair<SegmentUpdateInfoMap::iterator, bool> ret;
        ret = mUpdateMap.insert(std::make_pair(segUpdateInfo.updateSegId, segUpdateInfo));
        return ret.second;
    }

    Iterator CreateIterator() const { return Iterator(mUpdateMap); }

    size_t Size() const { return mUpdateMap.size(); }

public:
    SegmentUpdateInfoMap mUpdateMap;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AttributeUpdateInfo);
}} // namespace indexlib::index
