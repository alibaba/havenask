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
#ifndef __INDEXLIB_DOCUMENT_RECLAIMER_H
#define __INDEXLIB_DOCUMENT_RECLAIMER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/indexlib.h"
#include "indexlib/merger/document_reclaimer/document_deleter.h"
#include "indexlib/merger/document_reclaimer/index_reclaimer_param.h"
#include "indexlib/merger/multi_part_segment_directory.h"
#include "indexlib/util/metrics/MetricProvider.h"

namespace indexlib { namespace merger {

struct DocumentReclaimerFilter {
    std::string fieldName;
    std::string fieldValue;
};

class DocumentReclaimer
{
public:
    DocumentReclaimer(const SegmentDirectoryPtr& segmentDirectory, const config::IndexPartitionSchemaPtr& schema,
                      const config::IndexPartitionOptions& options, int64_t currentTs,
                      util::MetricProviderPtr metricProvider = util::MetricProviderPtr());
    ~DocumentReclaimer();

public:
    void Reclaim();

private:
    void ReclaimObsoleteDocs();
    void ReclaimSinglePartition(const SegmentDirectoryPtr& segDir);
    void ReclaimMultiPartition(const MultiPartSegmentDirectoryPtr& segDir);

private:
    void LoadIndexReclaimerParams();
    DocumentDeleterPtr CreateDocumentDeleter(const SegmentDirectoryPtr& segDir) const;
    void DeleteMatchedDocument(const SegmentDirectoryPtr& segDir, const DocumentDeleterPtr& docDeleter) const;
    void DumpSegmentAndCommitVersion(const SegmentDirectoryPtr& segDir, const DocumentDeleterPtr& docDeleter) const;
    void RemoveExpiredSegments(const SegmentDirectoryPtr& segDir);
    void ReclaimIndexTable(const SegmentDirectoryPtr& segDir);

private:
    SegmentDirectoryPtr mSegmentDirectory;
    config::IndexPartitionSchemaPtr mSchema;
    config::IndexPartitionOptions mOptions;
    std::string mDocReclaimConfigPath;
    std::vector<IndexReclaimerParam> mIndexReclaimerParams;
    std::string mReclaimParamJsonStr;
    int64_t mCurrentTs;
    util::MetricProviderPtr mMetricProvider;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DocumentReclaimer);
}} // namespace indexlib::merger

#endif //__INDEXLIB_DOCUMENT_RECLAIMER_H
