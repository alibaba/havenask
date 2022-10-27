#ifndef ISEARCH_JOIN_KERNEL_BASE_H
#define ISEARCH_JOIN_KERNEL_BASE_H

#include <ha3/sql/common/common.h>
#include <ha3/sql/data/Table.h>
#include <ha3/sql/attr/IndexInfo.h>
#include <ha3/sql/proto/SqlSearchInfoCollector.h>
#include <ha3/sql/ops/calc/CalcTable.h>
#include <ha3/sql/ops/join/JoinBase.h>
#include <navi/engine/Kernel.h>
#include <navi/resource/MemoryPoolResource.h>
#include <kmonitor/client/MetricsReporter.h>

BEGIN_HA3_NAMESPACE(sql);

class JoinKernelBase : public navi::Kernel {
public:
    static const int DEFAULT_BATCH_SIZE;
public:
    typedef std::vector<std::pair<size_t, size_t>> HashValues; // row : hash value

public:
    JoinKernelBase();
    ~JoinKernelBase();
public:
    const navi::KernelDef *getDef() const override;
    bool config(navi::KernelConfigContext &ctx) override;
    navi::ErrorCode init(navi::KernelInitContext &initContext) override;
    navi::ErrorCode compute(navi::KernelComputeContext &runContext) override;
protected:
    inline void joinRow(size_t leftIndex, size_t rightIndex);
    void reserveJoinRow(size_t rowCount);
    bool createHashMap(const TablePtr &table, size_t offset, size_t count, bool hashLeftTable);
    bool getHashValues(const TablePtr &table, size_t offset, size_t count,
                       const std::vector<std::string> &columnName, HashValues &values);
    bool getColumnHashValues(const TablePtr &table, size_t offset, size_t count,
                             const std::string &columnName,
                             HashValues &values);
    void combineHashValues(const HashValues &valuesA, HashValues &valuesB);
protected:
    virtual void reportMetrics();
    void patchHintInfo(const std::map<std::string, std::string> &hintsMap);
private:
    bool convertFields();
protected:
    autil::mem_pool::Pool *_pool;
    navi::MemoryPoolResource *_memoryPoolResource;

    std::string _joinType;
    std::string _semiJoinType;
    std::string _conditionJson;
    bool _isEquiJoin;
    bool _leftIsBuild;
    bool _shouldClearTable;
    std::string _equiConditionJson;
    std::string _remainConditionJson;
    std::vector<std::string> _leftInputFields;
    std::vector<std::string> _rightInputFields;
    std::vector<std::string> _outputFields;
    std::vector<std::string> _outputFieldsInternal;
    std::map<std::string, sql::IndexInfo> _leftIndexInfos;
    std::map<std::string, sql::IndexInfo> _rightIndexInfos;

    std::vector<std::string> _leftJoinColumns;
    std::vector<std::string> _rightJoinColumns;
    std::map<std::string, std::string> _hashHints;
    std::unordered_map<size_t, std::vector<size_t>> _hashJoinMap;
    std::map<std::string, std::pair<std::string, bool> > _output2InputMap;
    size_t _systemFieldNum;
    size_t _batchSize;
    kmonitor::MetricsReporter *_queryMetricsReporter;
    SqlSearchInfoCollector *_sqlSearchInfoCollector;
    std::vector<size_t> _tableAIndexes;
    std::vector<size_t> _tableBIndexes;
    size_t _joinIndex;
    JoinInfo _joinInfo;
    CalcTablePtr _calcTable; // filter result
    JoinBasePtr _joinPtr;
    int32_t _opId;
    std::map<std::string, std::string> _defaultValue;
protected:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(JoinKernelBase);

void JoinKernelBase::joinRow(size_t leftIndex, size_t rightIndex) {
    _tableAIndexes.push_back(leftIndex);
    _tableBIndexes.push_back(rightIndex);
}

END_HA3_NAMESPACE(sql);

#endif //ISEARCH_HASH_JOIN_KERNEL_H
