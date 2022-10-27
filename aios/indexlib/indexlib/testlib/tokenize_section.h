#ifndef __INDEXLIB_TOKENIZE_SECTION_H
#define __INDEXLIB_TOKENIZE_SECTION_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/partition/remote_access/index_field_convertor.h"

IE_NAMESPACE_BEGIN(testlib);

typedef partition::IndexFieldConvertor::TokenizeSection TokenizeSection; 
DEFINE_SHARED_PTR(TokenizeSection);

IE_NAMESPACE_END(testlib);

#endif //__INDEXLIB_TOKENIZE_SECTION_H
