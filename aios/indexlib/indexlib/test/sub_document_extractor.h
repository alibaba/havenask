#ifndef __INDEXLIB_TEST_SUB_DOCUMENT_EXTRACTOR_H
#define __INDEXLIB_TEST_SUB_DOCUMENT_EXTRACTOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/testlib/sub_document_extractor.h"

IE_NAMESPACE_BEGIN(test);

typedef index::SubDocumentExtractor SubDocumentExtractor;
DEFINE_SHARED_PTR(SubDocumentExtractor);

IE_NAMESPACE_END(test);

#endif //__INDEXLIB_TEST_SUB_DOCUMENT_EXTRACTOR_H
