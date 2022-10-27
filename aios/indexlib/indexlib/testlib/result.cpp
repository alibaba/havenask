#include "indexlib/testlib/result.h"


using namespace std;

IE_NAMESPACE_BEGIN(testlib);
IE_LOG_SETUP(testlib, Result);

Result::Result() 
    : mIsSubResult(false)
{
}

Result::~Result() 
{
}

IE_NAMESPACE_END(testlib);

