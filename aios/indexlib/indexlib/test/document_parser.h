#ifndef __INDEXLIB_TEST_DOCUMENT_PARSER_H
#define __INDEXLIB_TEST_DOCUMENT_PARSER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/test/raw_document.h"
#include "indexlib/test/result.h"
#include "indexlib/test/tokenize_section.h"
#include "indexlib/testlib/document_parser.h"

IE_NAMESPACE_BEGIN(test);

typedef testlib::DocumentParser DocumentParser; 
DEFINE_SHARED_PTR(DocumentParser);

IE_NAMESPACE_END(test);

#endif //__INDEXLIB_DOCUMENT_PARSER_H
