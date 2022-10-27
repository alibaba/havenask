#include <ha3/util/ChainedFixedSizePool.h>

using namespace std;
using namespace autil;

BEGIN_HA3_NAMESPACE(util);
HA3_LOG_SETUP(util, ChainedFixedSizePool);


ChainedFixedSizePool::ChainedFixedSizePool() { 
    _dataBlockSize = 0;
    _blockSize = 0;
    _allocator = NULL;
}

ChainedFixedSizePool::~ChainedFixedSizePool() { 
    delete _allocator;
    _allocator = NULL;
}

void ChainedFixedSizePool::init(uint32_t blockSize) {
    _dataBlockSize = blockSize;
    _blockSize = _dataBlockSize + sizeof(void*);
    _allocator = new FixedSizeAllocator(_blockSize);
}

ChainedBlockItem ChainedFixedSizePool::allocate(size_t size) {
    ChainedBlockItem item;
    if (size == 0) {
        return item;
    }
    uint32_t needBlockCount = (size - 1) / _dataBlockSize + 1; // upper
    autil::ScopedLock lock(_mutex);
    item.head = _allocator->allocate();
    void *tail = item.head;
    ChainedBlockItem::nextOf(tail) = NULL;
    for (uint32_t i = 1; i < needBlockCount; ++i) {
        void *buf = _allocator->allocate();
        ChainedBlockItem::nextOf(tail) = buf;
        tail = buf;
        ChainedBlockItem::nextOf(tail) = NULL;
    }
    return item;
}

void ChainedFixedSizePool::deallocate(ChainedBlockItem &item) {
    if (item.empty()) {
        return;
    }
    {
        autil::ScopedLock lock(_mutex);
        for (ChainedBlockItem::Iterator it = item.createIterator();
             it.hasNext(); )
        {
            void *buffer = it.getRawBuffer();
            it.next();
            _allocator->free(buffer);
        }
    }
    item.clear();
}

ChainedBlockItem ChainedFixedSizePool::createItem(
        const char *data, size_t size)
{
    ChainedBlockItem item = allocate(size);
    write(item, data, size);
    return item;
}

void ChainedFixedSizePool::write(ChainedBlockItem &item,
                                 const char *data, size_t size)
{
    uint32_t offset = 0;
    for (ChainedBlockItem::Iterator it = item.createIterator();
         it.hasNext(); it.next())
    {
        uint32_t writeSize = min((uint32_t)size - offset, _dataBlockSize);
        char *buffer = (char*)it.getDataBuffer();
        memcpy(buffer, data + offset, writeSize);
        offset += writeSize;
    }
    item.dataSize = size;
    assert(offset == size);
}

void ChainedFixedSizePool::read(
        ChainedBlockItem &item, char *data)
{
    uint32_t size = item.dataSize;
    uint32_t offset = 0;
    for (ChainedBlockItem::Iterator it = item.createIterator();
         it.hasNext(); it.next())
    {
        uint32_t writeSize = min(size - offset, _dataBlockSize);
        memcpy(data + offset, it.getDataBuffer(), writeSize);
        offset += writeSize;
    }
    assert(offset == size);
}

END_HA3_NAMESPACE(util);

