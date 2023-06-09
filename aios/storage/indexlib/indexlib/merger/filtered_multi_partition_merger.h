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
#ifndef __INDEXLIB_FILTERED_MULTI_PARTITION_MERGER_H
#define __INDEXLIB_FILTERED_MULTI_PARTITION_MERGER_H

#include <memory>

#include "indexlib/file_system/Directory.h"
#include "indexlib/index_base/segment/multi_part_segment_directory.h"
#include "indexlib/indexlib.h"
#include "indexlib/merger/doc_filter_creator.h"
#include "indexlib/merger/merge_meta.h"
#include "indexlib/merger/multi_partition_merger.h"
#include "indexlib/misc/common.h"
#include "indexlib/misc/log.h"

namespace indexlib { namespace merger {

class FilteredMultiPartitionMerger : public MultiPartitionMerger
{
public:
    FilteredMultiPartitionMerger(const config::IndexPartitionOptions& options, util::MetricProviderPtr metricProvider,
                                 const std::string& indexPluginPath, const DocFilterCreatorPtr& docFilterCreator,
                                 const std::string& epochId);
    ~FilteredMultiPartitionMerger();
    bool Init(const std::vector<std::string>& mergeSrc, versionid_t version, const std::string& mergeDest);
    std::string GetMergeMetaDir();
    MergeMetaPtr LoadMergeMeta(const std::string& mergeMetaDir, bool onlyLoadBasicInfo)
    {
        return mPartitionMerger->LoadMergeMeta(mergeMetaDir, onlyLoadBasicInfo);
    }
    void DoMerge(const indexlib::merger::MergeMetaPtr& mergeMeta, int32_t instanceId);
    void EndMerge(const indexlib::merger::MergeMetaPtr& mergeMeta, versionid_t alignVersion);
    void PrepareMergeMeta(int32_t parallelNum, const std::string& mergeMetaVersionDir);

private:
    void Merge(const std::vector<std::string>& mergeSrc, const std::string& mergeDest);
    indexlib::merger::MergeMetaPtr CreateMergeMeta(int32_t parallelNum, int64_t timestampInSecond);

    void GetFirstVersion(const file_system::DirectoryPtr& partDirectory, index_base::Version& version) override;
    void CheckSrcPath(const std::vector<file_system::DirectoryPtr>& mergeDirs,
                      const config::IndexPartitionSchemaPtr& schema, const index_base::PartitionMeta& partMeta,
                      std::vector<index_base::Version>& versions) override;

    void RewriteLoadConfigs(config::IndexPartitionOptions& options);
    int64_t getCacheBlockSize();

private:
    DocFilterCreatorPtr mDocFilterCreator;
    std::vector<std::string> mMergeSrcs;
    std::string mDestPath;
    versionid_t mTargetVersion;
    IndexPartitionMergerPtr mPartitionMerger;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(FilteredMultiPartitionMerger);
}} // namespace indexlib::merger

#endif //__INDEXLIB_FILTERED_MULTI_PARTITION_MERGER_H
