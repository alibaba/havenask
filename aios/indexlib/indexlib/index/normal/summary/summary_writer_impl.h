#ifndef __INDEXLIB_SUMMARY_WRITER_IMPL_H
#define __INDEXLIB_SUMMARY_WRITER_IMPL_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/summary/summary_writer.h"

DECLARE_REFERENCE_CLASS(index, LocalDiskSummaryWriter);

IE_NAMESPACE_BEGIN(index);

class SummaryWriterImpl : public SummaryWriter
{
public:
    SummaryWriterImpl();
    ~SummaryWriterImpl();
    DECLARE_SUMMARY_WRITER_IDENTIFIER(impl);

public:
    void Init(const config::SummarySchemaPtr& summarySchema,
              util::BuildResourceMetrics* buildResourceMetrics) override;
    bool AddDocument(const document::SerializedSummaryDocumentPtr& document) override;
    void Dump(const file_system::DirectoryPtr& directory,
              autil::mem_pool::PoolBase* dumpPool = NULL) override;
    const SummarySegmentReaderPtr CreateInMemSegmentReader() override;
    
private:
    typedef std::vector<LocalDiskSummaryWriterPtr> GroupWriterVec;
    GroupWriterVec mGroupWriterVec;
    config::SummarySchemaPtr mSummarySchema;

private:
    friend class SummaryWriterTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SummaryWriterImpl);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_SUMMARY_WRITER_IMPL_H
