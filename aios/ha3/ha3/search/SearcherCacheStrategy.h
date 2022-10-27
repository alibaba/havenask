#ifndef ISEARCH_SEARCHERCACHESTRATEGY_H
#define ISEARCH_SEARCHERCACHESTRATEGY_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>

BEGIN_HA3_NAMESPACE(common);
class Request;
class MultiErrorResult;
END_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(search);

class CacheResult;

class SearcherCacheStrategy
{
public:
    SearcherCacheStrategy() {};
    virtual ~SearcherCacheStrategy() {};
private:
    SearcherCacheStrategy(const SearcherCacheStrategy &);
    SearcherCacheStrategy& operator=(const SearcherCacheStrategy &);
public:
    virtual bool init(const common::Request *request, 
                      common::MultiErrorResult *errorResult) = 0;
    virtual bool genKey(const common::Request *request, uint64_t &key) = 0;
    virtual bool beforePut(uint32_t curTime, 
                           CacheResult *cacheResult) = 0;
    virtual uint32_t filterCacheResult(CacheResult *cacheResult) = 0;
    virtual uint32_t getCacheDocNum(const common::Request *request, 
                                    uint32_t docFound) = 0;
};

HA3_TYPEDEF_PTR(SearcherCacheStrategy);

END_HA3_NAMESPACE(search);

#endif //ISEARCH_SEARCHERCACHESTRATEGY_H
