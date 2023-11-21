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

#include "indexlib/common_define.h"
#include "indexlib/config/primary_key_index_config.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class PrimaryKeyLoadPlan
{
public:
    PrimaryKeyLoadPlan();
    virtual ~PrimaryKeyLoadPlan();

public:
    void Init(docid_t baseDocid, const config::PrimaryKeyIndexConfigPtr& indexConfig);

public:
    void AddSegmentData(const index_base::SegmentData& segmentData)
    {
        mSegmentDatas.push_back(segmentData);
        mDocCount += segmentData.GetSegmentInfo()->docCount;
    }

    bool CanDirectLoad() const;

    docid_t GetBaseDocId() const { return mBaseDocid; }
    size_t GetDocCount() const { return mDocCount; }

    // virtual for test
    virtual std::string GetTargetFileName() const;

    index_base::SegmentDataVector GetLoadSegmentDatas() const { return mSegmentDatas; }

    // virtual for test
    virtual file_system::DirectoryPtr GetTargetFileDirectory() const;

    size_t GetSegmentCount() const { return mSegmentDatas.size(); }
    void GetSegmentIdList(std::vector<segmentid_t>* segmentIds) const
    {
        for (auto& segmentData : mSegmentDatas) {
            segmentIds->push_back(segmentData.GetSegmentId());
        }
    }

private:
    docid_t mBaseDocid;
    size_t mDocCount;
    config::PrimaryKeyIndexConfigPtr mIndexConfig;
    index_base::SegmentDataVector mSegmentDatas;

private:
    IE_LOG_DECLARE();
};
DEFINE_SHARED_PTR(PrimaryKeyLoadPlan);
typedef std::vector<PrimaryKeyLoadPlanPtr> PrimaryKeyLoadPlanVector;
}} // namespace indexlib::index
