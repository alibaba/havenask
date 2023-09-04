#ifndef __INDEXLIB_TOKENIZE_SECTION_H
#define __INDEXLIB_TOKENIZE_SECTION_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/document/index_field_convertor.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace test {

typedef document::IndexFieldConvertor::TokenizeSection TokenizeSection;
DEFINE_SHARED_PTR(TokenizeSection);
}} // namespace indexlib::test

#endif //__INDEXLIB_TOKENIZE_SECTION_H
