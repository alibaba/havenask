#ifndef __INDEXLIB_TEST_TOKENIZE_SECTION_H
#define __INDEXLIB_TEST_TOKENIZE_SECTION_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/testlib/tokenize_section.h"

IE_NAMESPACE_BEGIN(test);

typedef testlib::TokenizeSection TokenizeSection;
DEFINE_SHARED_PTR(TokenizeSection);

IE_NAMESPACE_END(test);

#endif //__INDEXLIB_TEST_TOKENIZE_SECTION_H
