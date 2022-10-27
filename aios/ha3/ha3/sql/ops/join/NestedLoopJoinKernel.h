#pragma once

#include <ha3/sql/common/common.h>
#include <ha3/sql/data/Table.h>
#include <ha3/sql/ops/join/JoinKernelBase.h>
#include <navi/engine/Kernel.h>

BEGIN_HA3_NAMESPACE(sql);

class NestedLoopJoinKernel : public JoinKernelBase {
public:
    static const size_t DEFAULT_BUFFER_LIMIT_SIZE;
public:
    NestedLoopJoinKernel();
    ~NestedLoopJoinKernel();
public:
    const navi::KernelDef *getDef() const override;
    bool config(navi::KernelConfigContext &ctx) override;
    navi::ErrorCode compute(navi::KernelComputeContext &runContext) override;
private:
    bool doCompute(TablePtr &outputTable);
    bool getInput(navi::KernelComputeContext &runContext,
                  size_t index, TablePtr &input, bool &eof);
    bool waitFullTable();
    bool joinTable(size_t &joinedRowCount);
private:
    size_t _bufferLimitSize;
    bool _fullTableCreated;
    bool _leftFullTable;
    TablePtr _leftBuffer;
    TablePtr _rightBuffer;
    bool _leftEof;
    bool _rightEof;
};

HA3_TYPEDEF_PTR(NestedLoopJoinKernel);

END_HA3_NAMESPACE(sql);
