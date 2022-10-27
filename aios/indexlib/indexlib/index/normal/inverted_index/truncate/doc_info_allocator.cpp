#include "indexlib/index/normal/inverted_index/truncate/doc_info_allocator.h"
#include "indexlib/util/mmap_allocator.h"

using namespace std;
using namespace autil::mem_pool;

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, DocInfoAllocator);

const std::string DocInfoAllocator::DOCID_REFER_NAME = "docid";

DocInfoAllocator::DocInfoAllocator() 
{
    mDocInfoSize = 0;
    mAllocator = new MMapAllocator();
    mBufferPool = new RecyclePool(mAllocator, 
                                  DEFAULT_CHUNK_SIZE * 1024 * 1024);
    CreateReference<docid_t>(DOCID_REFER_NAME, ft_int32);
}

DocInfoAllocator::~DocInfoAllocator() 
{
    Release();

    delete mBufferPool;
    delete mAllocator;
    for (ReferenceMap::iterator it = mRefers.begin(); it != mRefers.end(); ++it)
    {
        delete it->second;
        it->second = NULL;
    }
    mRefers.clear();
}

DocInfo* DocInfoAllocator::Allocate()
{
    //TODO: recycle pool can only allocate memory larger than 8 bytes.
    if (mDocInfoSize < 8)
    {
        mDocInfoSize = 8;
    }
    return (DocInfo*)mBufferPool->allocate(mDocInfoSize);
}

void DocInfoAllocator::Deallocate(DocInfo *docInfo)
{
    return mBufferPool->deallocate(docInfo, mDocInfoSize);
}

void DocInfoAllocator::Release()
{
    if (mBufferPool)
    {
        mBufferPool->release();
    }
}

IE_NAMESPACE_END(index);

