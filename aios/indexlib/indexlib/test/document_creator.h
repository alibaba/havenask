#ifndef __INDEXLIB_TEST_DOCUMENTCREATOR_H
#define __INDEXLIB_TEST_DOCUMENTCREATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/document/document.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/testlib/document_creator.h"
#include <autil/StringUtil.h>

IE_NAMESPACE_BEGIN(test);

typedef testlib::DocumentCreator DocumentCreator; 
DEFINE_SHARED_PTR(DocumentCreator);

IE_NAMESPACE_END(test);

#endif //__INDEXLIB_TEST_DOCUMENTCREATOR_H
