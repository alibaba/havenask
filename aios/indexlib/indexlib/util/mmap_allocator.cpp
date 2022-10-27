#include "indexlib/util/mmap_allocator.h"

using namespace std;

IE_NAMESPACE_BEGIN(util);
IE_LOG_SETUP(util, MMapAllocator);

const size_t MMapAllocator::MUNMAP_SLICE_SIZE = 1024 * 1024;

MMapAllocator::MMapAllocator() 
{
}

MMapAllocator::~MMapAllocator() 
{
}

IE_NAMESPACE_END(util);

