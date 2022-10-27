#ifndef ISEARCH_CACHERESULT_H
#define ISEARCH_CACHERESULT_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/Result.h>
#include <ha3/index/index.h>
#include <ha3/search/CacheMinScoreFilter.h>
#include <autil/DataBuffer.h>
#include <indexlib/index/partition_info.h>

BEGIN_HA3_NAMESPACE(search);

class CacheHeader {
public:
    CacheHeader() {
        expireTime = INVALID_SEARCHER_CACHE_EXPIRE_TIME;
        time = 0;
    }
public:
    uint32_t expireTime; // s
    CacheMinScoreFilter minScoreFilter;
    uint32_t time; // s
};

class CacheResult
{
public:
    CacheResult();
    ~CacheResult();
private:
    CacheResult(const CacheResult &);
    CacheResult& operator=(const CacheResult &);
public:
    const CacheHeader* getHeader() const { return &_header; }
    CacheHeader* getHeader() { return &_header; }

    void setPartInfoHint(const index::PartitionInfoHint &partInfoHint) {
        _partInfoHint = partInfoHint;
    }
    const index::PartitionInfoHint& getPartInfoHint() const { return _partInfoHint; }

    const std::vector<globalid_t>& getGids() const { return _gids; }
    std::vector<globalid_t>& getGids() { return _gids; }

    const std::vector<globalid_t>& getSubDocGids() const { return _subDocGids; }
    std::vector<globalid_t>& getSubDocGids() { return _subDocGids; }

    void setResult(common::Result *result) { _result = result; }
    common::Result* getResult() const { return _result; }
    common::Result* stealResult();

    bool isTruncated() const { return _isTruncated; }
    void setTruncated(bool isTruncated) {_isTruncated = isTruncated;}

    bool useTruncateOptimizer() const { return _useTruncateOptimizer; }
    void setUseTruncateOptimizer(bool flag) { _useTruncateOptimizer = flag; }

    void serialize(autil::DataBuffer &dataBuffer) const;
    void serializeHeader(autil::DataBuffer &dataBuffer) const;
    void serializeResult(autil::DataBuffer &dataBuffer) const;

    void deserialize(autil::DataBuffer &dataBuffer,
                     autil::mem_pool::Pool *pool);
    void deserializeHeader(autil::DataBuffer &dataBuffer);
    void deserializeResult(autil::DataBuffer &dataBuffer,
                           autil::mem_pool::Pool *pool);
public:
    // for test
    void setGids(const std::vector<globalid_t> &gids)  { _gids = gids; }
private:
    CacheHeader _header;
    common::Result *_result;
    bool _isTruncated;
    bool _useTruncateOptimizer;
    index::PartitionInfoHint _partInfoHint;
    std::vector<globalid_t> _gids;
    std::vector<globalid_t> _subDocGids;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(CacheResult);

END_HA3_NAMESPACE(search);

#endif //ISEARCH_CACHERESULT_H
