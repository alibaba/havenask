#ifndef ISEARCH_SEARCHERCACHECONFIG_H
#define ISEARCH_SEARCHERCACHECONFIG_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <autil/legacy/jsonizable.h>
#include <ha3/config/TypeDefine.h>

BEGIN_HA3_NAMESPACE(config);

class SearcherCacheConfig : public autil::legacy::Jsonizable {
public:
    SearcherCacheConfig() {
        maxItemNum = 200000;
        maxSize = 0; //MB
        incDocLimit = std::numeric_limits<uint32_t>::max();
        incDeletionPercent = 100;
        latencyLimitInMs = 0.0; // ms
        minAllowedCacheDocNum = 0;
        maxAllowedCacheDocNum = std::numeric_limits<uint32_t>::max();
    }
    ~SearcherCacheConfig() {};
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {
        JSONIZE(json, "max_item_num", maxItemNum);
        JSONIZE(json, "max_size", maxSize);
        JSONIZE(json, "inc_doc_limit", incDocLimit);
        JSONIZE(json, "inc_deletion_percent", incDeletionPercent);
        JSONIZE(json, "latency_limit", latencyLimitInMs);
        JSONIZE(json, "min_allowed_cache_doc_num", minAllowedCacheDocNum);
        JSONIZE(json, "max_allowed_cache_doc_num", maxAllowedCacheDocNum);
    }
public:
    uint32_t maxItemNum;
    uint32_t maxSize;
    uint32_t incDocLimit;
    uint32_t incDeletionPercent;
    float latencyLimitInMs;
    uint32_t minAllowedCacheDocNum;
    uint32_t maxAllowedCacheDocNum;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(SearcherCacheConfig);

END_HA3_NAMESPACE(config);

#endif //ISEARCH_SEARCHERCACHECONFIG_H
