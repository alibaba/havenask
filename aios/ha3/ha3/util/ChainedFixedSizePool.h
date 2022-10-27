#ifndef ISEARCH_CHAINEDFIXEDSIZEPOOL_H
#define ISEARCH_CHAINEDFIXEDSIZEPOOL_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <autil/Lock.h>
#include <autil/FixedSizeAllocator.h>

BEGIN_HA3_NAMESPACE(util);

class ChainedBlockItem {
public:
    ChainedBlockItem() {
        head = NULL;
        dataSize = 0;
    }
public:
    inline static void *&nextOf(void *const pAddr) {
        return *(static_cast<void**>(pAddr));
    }
public:
    class Iterator {
    public:
        Iterator(void *head) {
            _cur = head;
        }
        ~Iterator() {}

    public:
        bool hasNext() { return _cur != NULL; }
        void next() { _cur = nextOf(_cur); }
        void *getDataBuffer() {
            return (void*)((char*)_cur + sizeof(void*));
        }
        void *getRawBuffer() {
            return _cur;
        }
    private:
        void *_cur;
    };
public:
    Iterator createIterator() {
        return Iterator(head);
    }
    bool empty() const {
        return head == NULL;
    }
    void clear() {
        head = NULL;
        dataSize = 0;
    }
public:
    // do not keep tail for now.
    void *head;
    uint32_t dataSize;
};

class ChainedFixedSizePool
{
public:
    ChainedFixedSizePool();
    ~ChainedFixedSizePool();
private:
    ChainedFixedSizePool(const ChainedFixedSizePool &);
    ChainedFixedSizePool& operator=(const ChainedFixedSizePool &);
public:
    void init(uint32_t blockSize);
public:
    ChainedBlockItem allocate(size_t size);
    void deallocate(ChainedBlockItem &item);
    size_t getBlockCount() const { return _allocator->getCount(); }
    size_t getMemoryUse() const { return _allocator->getCount() * _blockSize; }
public:
    ChainedBlockItem createItem(const char *data, size_t size);
    void write(ChainedBlockItem &item,
               const char *data, size_t size);
    void read(ChainedBlockItem &item, char *data);
private:
    uint32_t _dataBlockSize;
    uint32_t _blockSize;
    autil::ThreadMutex _mutex;
    autil::FixedSizeAllocator *_allocator;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(ChainedFixedSizePool);

END_HA3_NAMESPACE(util);

#endif //ISEARCH_CHAINEDFIXEDSIZEPOOL_H
