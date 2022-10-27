#ifndef __INDEXLIB_DOCUMENT_DEDUPER_HELPER_H
#define __INDEXLIB_DOCUMENT_DEDUPER_HELPER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/index_partition_options.h"

IE_NAMESPACE_BEGIN(common);

class DocumentDeduperHelper
{
public:
    DocumentDeduperHelper();
    ~DocumentDeduperHelper();
public:
    static bool DelayDedupDocument(const config::IndexPartitionOptions& options,
                                   const config::IndexConfigPtr& pkConfig);

};

DEFINE_SHARED_PTR(DocumentDeduperHelper);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_DOCUMENT_DEDUPER_HELPER_H
