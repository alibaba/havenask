#ifndef ISEARCH_MEMORYRESOURCECHECKER_H
#define ISEARCH_MEMORYRESOURCECHECKER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>

BEGIN_HA3_NAMESPACE(util);

// NOTE: getSysFreeMemSize is not correct
// currently, we use free + buffer + cache as freeMemSize
// but MAP_SHARED | MAP_LOCKED memory is also in cache 
// while it is not free.
// So, we only compare the memory size configured in SearchRule
// and the Res of Index/Searcher Worker
class MemoryResourceChecker
{
public:
    MemoryResourceChecker();
    virtual ~MemoryResourceChecker();
public:
    // @param :
    // memThreshold, unit in Byte
    bool getFreeMemSize(int64_t memThreshold, int64_t memGuardSize, 
                        int64_t& maxAvailaleMemSize);
private:
    virtual int64_t getProcResMemSize();
    virtual int64_t getSysFreeMemSize();
private:
    size_t _memPageSize;
    std::string _procStatmFile;
private:
    friend class MemoryResourceCheckerTest;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(MemoryResourceChecker);

END_HA3_NAMESPACE(util);

#endif //ISEARCH_MEMORYRESOURCECHECKER_H
