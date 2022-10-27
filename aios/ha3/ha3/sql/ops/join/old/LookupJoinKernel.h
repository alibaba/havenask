#ifndef ISEARCH_LOOKUP_JOIN_KERNEL_H
#define ISEARCH_LOOKUP_JOIN_KERNEL_H

#include <ha3/sql/common/common.h>
#include <ha3/sql/data/Table.h>
#include <ha3/sql/data/TableUtil.h>
#include <ha3/sql/resource/SqlBizResource.h>
#include <ha3/sql/resource/SqlQueryResource.h>
#include <ha3/sql/ops/join/JoinKernelBase.h>
#include <ha3/sql/ops/util/KernelUtil.h>
#include <navi/engine/Kernel.h>
#include <navi/resource/MemoryPoolResource.h>

BEGIN_HA3_NAMESPACE(sql);

class LookupJoinKernel : public JoinKernelBase {
public:
    LookupJoinKernel();
    ~LookupJoinKernel();
public:
    navi::ErrorCode init(navi::KernelInitContext &initContext) override;
    navi::ErrorCode compute(navi::KernelComputeContext &runContext) override;
protected:
    virtual navi::ErrorCode doInit(SqlBizResource *bizResource, SqlQueryResource *queryResource) = 0;
    bool doCompute(navi::KernelComputeContext &runContext, TablePtr leftInput);
    bool lookupJoin(TablePtr leftTable);
    virtual bool singleJoin(ColumnPtr joinColumn, ValueType vt, size_t rowCount) = 0;
    virtual bool multiJoin(ColumnPtr joinColumn, ValueType vt, size_t rowCount) = 0;
protected:
    autil::mem_pool::Pool *_pool;
    navi::MemoryPoolResource *_memoryPoolResource;
    std::map<std::string, std::string> _joinField;
    TablePtr _rightTable;
};

HA3_TYPEDEF_PTR(LookupJoinKernel);

END_HA3_NAMESPACE(sql);

#endif //ISEARCH_LOOK_UP_JOIN_KERNEL_H
