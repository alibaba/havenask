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
#ifndef __INDEXLIB_SEGMENT_DATA_BASE_H
#define __INDEXLIB_SEGMENT_DATA_BASE_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index_base {

class SegmentDataBase
{
public:
    SegmentDataBase();
    SegmentDataBase(const SegmentDataBase& other);
    ~SegmentDataBase();

public:
    SegmentDataBase& operator=(const SegmentDataBase& segData);
    void SetBaseDocId(exdocid_t baseDocId) { mBaseDocId = baseDocId; }
    exdocid_t GetBaseDocId() const { return mBaseDocId; }

    void SetSegmentId(segmentid_t segmentId) { mSegmentId = segmentId; }
    segmentid_t GetSegmentId() const { return mSegmentId; }
    void SetSegmentDirName(const std::string& segDirName) { mSegmentDirName = segDirName; }
    const std::string& GetSegmentDirName() const { return mSegmentDirName; }
    void SetLifecycle(const std::string& lifecycle) { mLifecycle = lifecycle; }
    const std::string& GetLifecycle() const { return mLifecycle; }

protected:
    exdocid_t mBaseDocId;
    segmentid_t mSegmentId;
    std::string mSegmentDirName;
    std::string mLifecycle;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SegmentDataBase);
}} // namespace indexlib::index_base

#endif //__INDEXLIB_SEGMENT_DATA_BASE_H
