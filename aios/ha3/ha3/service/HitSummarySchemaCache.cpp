#include <ha3/service/HitSummarySchemaCache.h>

using namespace std;
using namespace autil;
using namespace suez::turing;

USE_HA3_NAMESPACE(config);

BEGIN_HA3_NAMESPACE(service);
HA3_LOG_SETUP(service, HitSummarySchemaCache);

HitSummarySchemaCache::HitSummarySchemaCache(const ClusterTableInfoMapPtr &clusterTableInfoMapPtr) {
    if (!clusterTableInfoMapPtr) {
        return;
    }
    for (ClusterTableInfoMap::const_iterator it = clusterTableInfoMapPtr->begin();
         it != clusterTableInfoMapPtr->end(); ++it)
    {
        const string &clusterName = it->first;
        HitSummarySchemaPtr hitSummarySchemaPtr(new HitSummarySchema(it->second.get()));
        _hitSummarySchemaMap[clusterName] = hitSummarySchemaPtr;
    }
}

HitSummarySchemaCache::~HitSummarySchemaCache() {
}

void HitSummarySchemaCache::setHitSummarySchema(const string &clusterName,
        HitSummarySchema* hitSummarySchema)
{
    HitSummarySchemaPtr curHitSummarySchemaPtr = getHitSummarySchema(clusterName);

    if (NULL != curHitSummarySchemaPtr.get()
        && hitSummarySchema->getSignature() == curHitSummarySchemaPtr->getSignatureNoCalc())
    {
        return;
    }

    HitSummarySchemaPtr hitSummarySchemaPtr(hitSummarySchema->clone());
    ScopedWriteLock lock(_lock);
    _hitSummarySchemaMap[clusterName] = hitSummarySchemaPtr;
}

HitSummarySchemaPtr HitSummarySchemaCache::getHitSummarySchema(
        const string &clusterName) const
{
    ScopedReadLock lock(_lock);
    map<string, HitSummarySchemaPtr>::const_iterator it =
        _hitSummarySchemaMap.find(clusterName);
    if (it == _hitSummarySchemaMap.end()) {
        return HitSummarySchemaPtr();
    }
    return it->second;
}

void HitSummarySchemaCache::addHitSummarySchema(const string &clusterName,
        const HitSummarySchema* hitSummarySchema)
{
    HitSummarySchemaPtr hitSummarySchemaPtr(hitSummarySchema->clone());
    _hitSummarySchemaMap[clusterName] = hitSummarySchemaPtr;
}

END_HA3_NAMESPACE(service);
