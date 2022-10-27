#ifndef ISEARCH_MULTIHITCOLLECTOR_H
#define ISEARCH_MULTIHITCOLLECTOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/rank/HitCollectorBase.h>
#include <suez/turing/expression/framework/AttributeExpression.h>

BEGIN_HA3_NAMESPACE(rank);

class MultiHitCollector : public HitCollectorBase
{
public:
    MultiHitCollector(autil::mem_pool::Pool *pool,
                      const common::Ha3MatchDocAllocatorPtr &allocatorPtr,
                      suez::turing::AttributeExpression *expr);
    virtual ~MultiHitCollector();
private:
    MultiHitCollector(const MultiHitCollector &);
    MultiHitCollector& operator=(const MultiHitCollector &);
public:
    uint32_t doGetItemCount() const override;
    HitCollectorType getType() const override {
        return HCT_MULTI;
    }
    matchdoc::MatchDoc top() const override;
    const ComboComparator *getComparator() const override {
        assert(false);
        return NULL;
    }
public:
    void addHitCollector(HitCollectorBase *hitCollector) {
        _hitCollectors.push_back(hitCollector);
    }
    const std::vector<HitCollectorBase*> &getHitCollectors() const {
        return _hitCollectors;
    }
protected:
    matchdoc::MatchDoc collectOneDoc(matchdoc::MatchDoc matchDoc) override;
    void doStealAllMatchDocs(autil::mem_pool::PoolVector<matchdoc::MatchDoc> &target) override;
private:
    std::vector<HitCollectorBase *> _hitCollectors;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(MultiHitCollector);

END_HA3_NAMESPACE(rank);

#endif //ISEARCH_MULTIHITCOLLECTOR_H
