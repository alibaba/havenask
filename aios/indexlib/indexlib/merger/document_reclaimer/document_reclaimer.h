#ifndef __INDEXLIB_DOCUMENT_RECLAIMER_H
#define __INDEXLIB_DOCUMENT_RECLAIMER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/misc/metric_provider.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/merger/multi_part_segment_directory.h"
#include "indexlib/merger/document_reclaimer/document_deleter.h"
#include "indexlib/merger/document_reclaimer/index_reclaimer_param.h"

IE_NAMESPACE_BEGIN(merger);

struct DocumentReclaimerFilter
{
    std::string fieldName;
    std::string fieldValue;
};

class DocumentReclaimer
{
public:
    DocumentReclaimer(const SegmentDirectoryPtr& segmentDirectory,
                      const config::IndexPartitionSchemaPtr& schema,
                      const config::IndexPartitionOptions& options,
                      int64_t currentTs,
                      misc::MetricProviderPtr metricProvider = misc::MetricProviderPtr());
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
    void DeleteMatchedDocument(const SegmentDirectoryPtr& segDir,
                               const DocumentDeleterPtr& docDeleter) const;
    void DumpSegmentAndCommitVersion(const SegmentDirectoryPtr& segDir,
                               const DocumentDeleterPtr& docDeleter) const;
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
    misc::MetricProviderPtr mMetricProvider;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DocumentReclaimer);

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_DOCUMENT_RECLAIMER_H
