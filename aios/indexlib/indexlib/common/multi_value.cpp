#include "indexlib/common/multi_value.h"

using namespace std;

IE_NAMESPACE_BEGIN(common);
IE_LOG_SETUP(common, MultiValue);

MultiValue::MultiValue() 
{
    mSize = 0;
}

MultiValue::~MultiValue() 
{
    for (size_t i = 0; i < mAtomicValues.size(); ++i)
    {
        delete mAtomicValues[i];
    }
    mAtomicValues.clear();
}

IE_NAMESPACE_END(common);

