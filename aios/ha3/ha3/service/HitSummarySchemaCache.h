#ifndef ISEARCH_HITSUMMARYSCHEMACACHE_H
#define ISEARCH_HITSUMMARYSCHEMACACHE_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <autil/Lock.h>
#include <ha3/config/HitSummarySchema.h>
#include <suez/turing/expression/util/TableInfo.h>

BEGIN_HA3_NAMESPACE(service);

class HitSummarySchemaCache
{
public:
    HitSummarySchemaCache(const suez::turing::ClusterTableInfoMapPtr &clusterTableInfoMapPtr);
    ~HitSummarySchemaCache();
private:
    HitSummarySchemaCache(const HitSummarySchemaCache &);
    HitSummarySchemaCache& operator=(const HitSummarySchemaCache &);
public:
    void setHitSummarySchema(const std::string &clusterName,
                             config::HitSummarySchema* hitSummarySchema);
    config::HitSummarySchemaPtr getHitSummarySchema(const std::string &clusterName) const;

public:
    // only for test
    void addHitSummarySchema(const std::string &clusterName,
                             const config::HitSummarySchema* hitSummarySchema);
private:
    mutable autil::ReadWriteLock _lock;
    std::map<std::string, config::HitSummarySchemaPtr> _hitSummarySchemaMap;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(HitSummarySchemaCache);

END_HA3_NAMESPACE(service);

#endif //ISEARCH_HITSUMMARYSCHEMACACHE_H
