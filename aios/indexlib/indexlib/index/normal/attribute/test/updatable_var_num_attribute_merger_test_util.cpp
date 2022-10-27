#include "updatable_var_num_attribute_merger_test_util.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_BEGIN(index);

MockUpdatableVarNumAttributeSegmentReader::~MockUpdatableVarNumAttributeSegmentReader()
{
    for (size_t i = 0; i < mBufferVec.size(); ++i)
    {
        delete[] mBufferVec[i];
    }
}

uint8_t *MockUpdatableVarNumAttributeSegmentReader::AllocateBuffer(size_t size)
{
    uint8_t *buffer = new uint8_t[size];
    mBufferVec.push_back(buffer);
    return buffer;
}


IE_NAMESPACE_END(index);

