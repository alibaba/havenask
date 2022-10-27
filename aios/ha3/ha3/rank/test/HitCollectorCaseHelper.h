#ifndef ISEARCH_HITCOLLECTORCASEHELPER_H
#define ISEARCH_HITCOLLECTORCASEHELPER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <suez/turing/expression/framework/AttributeExpression.h>
#include <ha3/rank/HitCollectorBase.h>
#include <matchdoc/MatchDocAllocator.h>
#include <ha3/rank/test/FakeRankExpression.h>
#include <ha3/rank/DistinctHitCollector.h>

BEGIN_HA3_NAMESPACE(rank);

class HitCollectorCaseHelper
{
public:
    HitCollectorCaseHelper();
    ~HitCollectorCaseHelper();
private:
    HitCollectorCaseHelper(const HitCollectorCaseHelper &);
    HitCollectorCaseHelper& operator=(const HitCollectorCaseHelper &);
public:
    static void makeData(HitCollectorBase *hitCollector, const std::string &matchDocStr,
                         const std::vector<FakeRankExpression*> &attrExprs,
                         const common::Ha3MatchDocAllocatorPtr &allocatorPtr);

    static void checkPop(DistinctHitCollector *hitCollector, const std::string &popOpteratorStr,
                         const common::Ha3MatchDocAllocatorPtr &allocatorPtr);
    static void checkLeftMatchDoc(HitCollectorBase *hitCollector, const std::string &popOpteratorStr,
                                  const common::Ha3MatchDocAllocatorPtr &allocatorPtr);
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(HitCollectorCaseHelper);

END_HA3_NAMESPACE(rank);

#endif //ISEARCH_HITCOLLECTORCASEHELPER_H
