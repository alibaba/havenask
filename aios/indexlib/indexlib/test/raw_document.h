#ifndef __INDEXLIB_TEST_RAW_DOCUMENT_H
#define __INDEXLIB_TEST_RAW_DOCUMENT_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/document/locator.h"
#include "indexlib/testlib/raw_document.h"

IE_NAMESPACE_BEGIN(test);

typedef testlib::RawDocument RawDocument; 
DEFINE_SHARED_PTR(RawDocument);

IE_NAMESPACE_END(test);

#endif //__INDEXLIB_TEST_RAW_DOCUMENT_H
