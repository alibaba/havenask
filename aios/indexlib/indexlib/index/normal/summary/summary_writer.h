#ifndef __INDEXLIB_SUMMARY_WRITER_H
#define __INDEXLIB_SUMMARY_WRITER_H

#include <tr1/memory>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(util, BuildResourceMetrics);
DECLARE_REFERENCE_CLASS(config, SummarySchema);
DECLARE_REFERENCE_CLASS(document, SerializedSummaryDocument);
DECLARE_REFERENCE_CLASS(index, SummarySegmentReader);
DECLARE_REFERENCE_CLASS(file_system, Directory);

IE_NAMESPACE_BEGIN(index);

#define DECLARE_SUMMARY_WRITER_IDENTIFIER(id)                              \
    static std::string Identifier() { return std::string("summary.writer." #id);} \
    virtual std::string GetIdentifier() const { return Identifier();}    

class SummaryWriter
{
public:
    SummaryWriter() {}
    virtual ~SummaryWriter() {}

public:
    virtual void Init(const config::SummarySchemaPtr& summarySchema,
                      util::BuildResourceMetrics* buildResourceMetrics) = 0;
    virtual bool AddDocument(const document::SerializedSummaryDocumentPtr& document) = 0;
    //virtual void Dump(const std::string& dir) = 0;
    virtual void Dump(const file_system::DirectoryPtr& dir,
                      autil::mem_pool::PoolBase* dumpPool = NULL) = 0;
    virtual const SummarySegmentReaderPtr CreateInMemSegmentReader() = 0;

private:
    IE_LOG_DECLARE();
};

typedef std::tr1::shared_ptr<SummaryWriter> SummaryWriterPtr;

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_SUMMARY_WRITER_H
