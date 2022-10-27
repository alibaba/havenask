#ifndef __INDEXLIB_TEST_RESULT_H
#define __INDEXLIB_TEST_RESULT_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/testlib/result.h"

IE_NAMESPACE_BEGIN(test);

typedef testlib::Result Result;
DEFINE_SHARED_PTR(Result);

IE_NAMESPACE_END(test);

#endif //__INDEXLIB_TEST_RESULT_H
