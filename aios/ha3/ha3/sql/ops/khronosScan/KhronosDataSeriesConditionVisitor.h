#pragma once

#include <ha3/sql/ops/khronosScan/KhronosDataConditionVisitor.h>

BEGIN_HA3_NAMESPACE(sql);

class KhronosDataSeriesConditionVisitor : public KhronosDataConditionVisitor
{
public:
    KhronosDataSeriesConditionVisitor(autil::mem_pool::Pool *pool,
            const IndexInfoMapType &indexInfos);
    ~KhronosDataSeriesConditionVisitor();
public:
    void visitLeafCondition(LeafCondition *condition) override;
private:
    void parseAsTimestampUdf(const autil_rapidjson::SimpleValue &leafCondition);
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(KhronosDataSeriesConditionVisitor);

END_HA3_NAMESPACE(sql);
