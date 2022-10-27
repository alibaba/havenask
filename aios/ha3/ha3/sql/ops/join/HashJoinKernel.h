#ifndef ISEARCH_HASH_JOIN_KERNEL_H
#define ISEARCH_HASH_JOIN_KERNEL_H

#include <ha3/sql/common/common.h>
#include <ha3/sql/data/Table.h>
#include <ha3/sql/ops/join/JoinKernelBase.h>
#include <navi/engine/Kernel.h>

BEGIN_HA3_NAMESPACE(sql);

class HashJoinKernel : public JoinKernelBase {
public:
    static const size_t DEFAULT_BUFFER_LIMIT_SIZE;
public:
    HashJoinKernel();
    ~HashJoinKernel();
public:
    const navi::KernelDef *getDef() const override;
    bool config(navi::KernelConfigContext &ctx) override;
    navi::ErrorCode compute(navi::KernelComputeContext &runContext) override;
private:
    bool doCompute(TablePtr &outputTable);
    bool getInput(navi::KernelComputeContext &runContext,
                  size_t index, TablePtr &input, bool &eof);
    bool tryCreateHashMap();
    bool joinTable(size_t &joinedRowCount);
    size_t makeHashJoin(const HashValues &values);
private:
    size_t _bufferLimitSize;
    bool _hashMapCreated;
    bool _hashLeftTable;
    TablePtr _leftBuffer;
    TablePtr _rightBuffer;
    bool _leftEof;
    bool _rightEof;
};

HA3_TYPEDEF_PTR(HashJoinKernel);

END_HA3_NAMESPACE(sql);

#endif //ISEARCH_HASH_JOIN_KERNEL_H
