#ifndef ISEARCH_LOOKUP_JOIN_KERNEL_H
#define ISEARCH_LOOKUP_JOIN_KERNEL_H

#include <set>
#include <ha3/sql/common/common.h>
#include <ha3/sql/data/Table.h>
#include <ha3/sql/data/TableUtil.h>
#include <ha3/sql/ops/join/JoinKernelBase.h>
#include <ha3/sql/ops/scan/ScanBase.h>
#include <ha3/sql/ops/util/KernelUtil.h>
#include <ha3/common/Query.h>
#include <ha3/common/Term.h>
#include <ha3/common/TableQuery.h>
#include <navi/engine/Kernel.h>

BEGIN_HA3_NAMESPACE(sql);

class LookupJoinKernel : public JoinKernelBase {
public:
    LookupJoinKernel();
    ~LookupJoinKernel();
public:
    const navi::KernelDef *getDef() const override;
    bool config(navi::KernelConfigContext &ctx) override;
    navi::ErrorCode init(navi::KernelInitContext &initContext) override;
    navi::ErrorCode compute(navi::KernelComputeContext &runContext) override;
    void setEnableRowDeduplicate(bool enableRowDeduplicate);
protected:
    bool doLookupJoin(TablePtr &output);
    bool emptyJoin(const TablePtr &inputTable, TablePtr &outputTable);
    bool joinTable(const TablePtr &inputTable, size_t offset, size_t count,
                   const TablePtr &streamOutput,TablePtr &outputTable);
    bool joinTable(const TablePtr &inputTable, size_t offset, size_t count,
                   const TablePtr &streamTable);

    StreamQueryPtr genFinalStreamQuery(const TablePtr &input, size_t offset, size_t count);
    bool genStreamKeys(const TablePtr &input, size_t offset, size_t count, std::vector<std::string>& pks);
    bool getPkTriggerField(std::string& triggerField);
    bool getPkTriggerField(const std::vector<std::string>& inputFields,
                           const std::vector<std::string>& joinFields,
                           const std::map<std::string, sql::IndexInfo>& indexInfoMap,
                           std::string& triggerField);
    common::QueryPtr genStreamQuery(const TablePtr &input, size_t offset, size_t count);
    common::QueryPtr genStreamQuery(Row row);
    common::QueryPtr genStreamQuery(const TablePtr &input, Row row,
                                    const std::vector<std::string> &inputFields,
                                    const std::vector<std::string> &joinFields,
                                    const std::map<std::string, sql::IndexInfo> &indexInfos);
    common::QueryPtr genStreamQuery(const TablePtr &input, Row row,
                                    const std::string &inputField,
                                    const std::string &joinField,
                                    const std::string& indexName,
                                    const std::string& indexType);
    bool genStreamQueryTerm(const TablePtr &input, Row row,
                            const std::string &inputField,
                            std::vector<std::string> &termVec);
    void patchLookupHintInfo(const std::map<std::string, std::string> &hintsMap);
    bool canTurncate();
    common::QueryPtr genTableQuery(const TablePtr &input, size_t offset, size_t count);
    common::ColumnTerm* makeColumnTerm(
        const TablePtr &input,
        const std::string &inputField,
        const std::string &joinField,
        size_t offset,
        size_t endOffset);
    common::QueryPtr genTableQuery(
        const TablePtr &input, size_t offset, size_t count,
        const std::vector<std::string> &inputFields,
        const std::vector<std::string> &joinFields,
        const std::map<std::string, sql::IndexInfo> &indexInfos);
    bool genrowGroupKey(
        const TablePtr &input,
        const std::vector<ColumnPtr> &singleTermCols,
        size_t offset,
        size_t endOffset,
        std::vector<std::string> &rowGroupKey);
    bool genSingleTermColumns(
        const TablePtr &input,
        const std::vector<ColumnPtr> &singleTermCols,
        const std::vector<size_t> &singleTermID,
        const std::vector<std::string> &joinFields,
        const std::unordered_map<std::string, std::vector<size_t>> &rowGroups,
        size_t rowCount,
        std::vector<common::ColumnTerm*> &terms);
    bool genMultiTermColumns(
        const TablePtr &input,
        const std::vector<ColumnPtr> &MultiTermCols,
        const std::vector<size_t> &multiTermID,
        const std::vector<std::string> &joinFields,
        const std::unordered_map<std::string, std::vector<size_t>> &rowGroups,
        size_t rowCount,
        std::vector<common::ColumnTerm*> &terms);
    common::QueryPtr genTableQueryWithRowOptimized(
        const TablePtr &input, size_t offset, size_t count,
        const std::vector<std::string> &inputFields,
        const std::vector<std::string> &joinFields,
        const std::map<std::string, sql::IndexInfo> &indexInfos);
protected:
    ScanInitParam _initParam;
    ScanBasePtr _scanBase;
    TablePtr _inputTable;
    bool _lastScanEof;
    bool _inputEof;
    size_t _lookupBatchSize;
    size_t _lookupTurncateThreshold;
    size_t _hasJoinedCount;
    std::set<std::string> _disableCacheFields;
    bool _enableRowDeduplicate = false;
};

HA3_TYPEDEF_PTR(LookupJoinKernel);

END_HA3_NAMESPACE(sql);

#endif //ISEARCH_LOOK_UP_JOIN_KERNEL_H
