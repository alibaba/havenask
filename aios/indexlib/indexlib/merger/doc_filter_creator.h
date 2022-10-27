#ifndef __INDEXLIB_DOC_FILTER_CREATOR_H
#define __INDEXLIB_DOC_FILTER_CREATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/misc/log.h"
#include "indexlib/misc/common.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/merger/doc_filter.h"
#include "indexlib/merger/segment_directory.h"

IE_NAMESPACE_BEGIN(merger);

class DocFilterCreator
{
public:
    DocFilterCreator() {}
    virtual ~DocFilterCreator() {}
public:
    virtual DocFilterPtr CreateDocFilter(
        const SegmentDirectoryPtr& segDir,
        const config::IndexPartitionSchemaPtr& schema,
        const config::IndexPartitionOptions& options) = 0;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DocFilterCreator);

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_DOC_FILTER_CREATOR_H
