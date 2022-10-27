#ifndef __INDEXLIB_SUMMARY_MAKER_H
#define __INDEXLIB_SUMMARY_MAKER_H

#include <tr1/memory>
#include <vector>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index/normal/summary/summary_writer.h"
#include "indexlib/document/index_document/normal_document/summary_document.h"

IE_NAMESPACE_BEGIN(index);

class SummaryMaker
{
public:
    typedef std::vector<document::SummaryDocumentPtr> DocumentArray;

public:
    SummaryMaker();
    ~SummaryMaker();

public:
    static document::SummaryDocumentPtr MakeOneDocument(docid_t docId, 
            const config::IndexPartitionSchemaPtr& indexPartitionSchema,
            autil::mem_pool::Pool *pool);

    static void BuildOneSegment(const file_system::DirectoryPtr& segDirectory, 
                                segmentid_t segId, 
                                config::IndexPartitionSchemaPtr& schema,
                                uint32_t docCount, autil::mem_pool::Pool *pool,
                                DocumentArray& answerDocArray);

    static SummaryWriterPtr BuildOneSegmentWithoutDump(uint32_t docCount, 
            const config::IndexPartitionSchemaPtr& schema, autil::mem_pool::Pool *pool,
            DocumentArray& answerDocArray);
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SummaryMaker);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_SUMMARY_MAKER_H
