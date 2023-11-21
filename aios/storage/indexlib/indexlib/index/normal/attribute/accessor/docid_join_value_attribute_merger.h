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
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/index/normal/attribute/accessor/attribute_merger.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class DocidJoinValueAttributeMerger : public AttributeMerger
{
public:
    DocidJoinValueAttributeMerger();
    ~DocidJoinValueAttributeMerger();
    DECLARE_ATTRIBUTE_MERGER_IDENTIFIER(DocidJoinValue);

public:
    void Init(const config::AttributeConfigPtr& attrConfig) override;

    void Merge(const MergerResource& resource, const index_base::SegmentMergeInfos& segMergeInfos,
               const index_base::OutputSegmentMergeInfos& outputSegMergeInfos) override;

    void SortByWeightMerge(const MergerResource& resource, const index_base::SegmentMergeInfos& segMergeInfos,
                           const index_base::OutputSegmentMergeInfos& outputSegMergeInfos) override;

    int64_t EstimateMemoryUse(const SegmentDirectoryBasePtr& segDir, const MergerResource& resource,
                              const index_base::SegmentMergeInfos& segMergeInfos,
                              const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,
                              bool isSortedMerge) const override;

    virtual bool EnableMultiOutputSegmentParallel() const override { return false; }

private:
    file_system::FileWriterPtr CreateDataFileWriter(const file_system::DirectoryPtr& attributeDir);
    void MainToSubMerge(const MergerResource& resource, const std::vector<file_system::FileWriterPtr>& dataFiles);
    void SubToMainMerge(const MergerResource& resource, const std::vector<file_system::FileWriterPtr>& dataFiles);

private:
    static docid_t GetBaseDocId(const std::vector<docid_t>& docids, size_t segIdx)
    {
        if (docids.empty()) {
            return 0;
        }
        return docids[segIdx];
    }

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DocidJoinValueAttributeMerger);
}} // namespace indexlib::index
