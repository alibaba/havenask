#pragma once

#include <ha3/util/Log.h>
#include <ha3/sql/common/common.h>
#include <ha3/sql/ops/khronosScan/KhronosDataConditionVisitor.h>
#include <ha3/sql/attr/IndexInfo.h>
#include <khronos_table_interface/TimeRange.h>
#include <khronos_table_interface/SearchInterface.h>
#include <autil/mem_pool/Pool.h>

BEGIN_HA3_NAMESPACE(sql);

class KhronosDataPointConditionVisitor : public KhronosDataConditionVisitor
{
public:
    KhronosDataPointConditionVisitor(autil::mem_pool::Pool *pool,
            const IndexInfoMapType &indexInfos);
    ~KhronosDataPointConditionVisitor();
public:
    void visitLeafCondition(LeafCondition *condition) override;
protected:
    void parseAsTimestamp(const autil_rapidjson::SimpleValue &leftIn,
                          const autil_rapidjson::SimpleValue &rightIn,
                          const std::string &op,
                          const std::string &rawOp);

private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(KhronosDataPointConditionVisitor);

END_HA3_NAMESPACE(sql);
