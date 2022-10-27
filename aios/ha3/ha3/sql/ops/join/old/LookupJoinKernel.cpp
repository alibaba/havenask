#include <ha3/sql/ops/join/LookupJoinKernel.h>
#include <ha3/sql/ops/join/HashJoinConditionVisitor.h>
#include <ha3/sql/ops/condition/ConditionParser.h>

using namespace std;
using namespace matchdoc;

BEGIN_HA3_NAMESPACE(sql);

LookupJoinKernel::LookupJoinKernel()
    : _pool(nullptr)
    , _memoryPoolResource(nullptr)
{
}

LookupJoinKernel::~LookupJoinKernel() {
    _memoryPoolResource = nullptr;
}

navi::ErrorCode LookupJoinKernel::init(navi::KernelInitContext& context) {
    auto bizResource = context.getResource<SqlBizResource>("SqlBizResource");
    KERNEL_REQUIRES(bizResource, "get sql biz resource failed");
    auto queryResource = context.getResource<SqlQueryResource>("SqlQueryResource");
    KERNEL_REQUIRES(bizResource, "get sql query resource failed");
    _pool = queryResource->getPool();
    KERNEL_REQUIRES(_pool, "get pool failed");
    _memoryPoolResource = context.getResource<navi::MemoryPoolResource>(
        navi::RESOURCE_MEMORY_POOL_URI);
    KERNEL_REQUIRES(_memoryPoolResource, "get mem pool resource failed");

    // tmp use hash cond visitor
    ConditionParser parser(_pool);
    ConditionPtr condition;
    if (!parser.parseCondition(_conditionJson, condition) || !condition) {
        SQL_LOG(ERROR, "parse condition [%s] failed", _conditionJson.c_str());
        return navi::EC_ABORT;
    }
    HashJoinConditionVisitor visitor(_output2InputMap);
    condition->accept(&visitor);
    if (visitor.isError()) {
        SQL_LOG(ERROR, "visit condition failed, error info [%s]",
                        visitor.errorInfo().c_str());
        return navi::EC_ABORT;
    }
    auto leftJoinColumns = visitor.getLeftJoinColumns();
    auto rightJoinColumns = visitor.getRightJoinColumns();
    if (leftJoinColumns.empty() || leftJoinColumns.size() != rightJoinColumns.size()) {
        SQL_LOG(ERROR, "left join column size [%zu] not match right one [%zu]",
                        leftJoinColumns.size(), rightJoinColumns.size());
        return navi::EC_ABORT;
    }
    _joinField.insert(make_pair(leftJoinColumns[0], rightJoinColumns[0]));

    _joinInfo.set_kernelname(getKernelName());
    _joinInfo.set_nodename(getNodeName());
    string keyStr = getKernelName() + "__" + _joinField.begin()->first
                    + "__" + _joinField.begin()->second;
    _joinInfo.set_hashkey(autil::HashAlgorithm::hashString(keyStr.c_str(), keyStr.size(), 1));
    return doInit(bizResource, queryResource);
}

navi::ErrorCode LookupJoinKernel::compute(navi::KernelComputeContext &runContext) {
    incComputeTimes();
    uint64_t beginTime = TimeUtility::currentTime();
    navi::PortIndex leftPort(0, navi::INVALID_INDEX);
    bool leftEof = false;
    navi::DataPtr leftData;
    runContext.getInput(leftPort, leftData, leftEof);
    if (leftData != nullptr) {
        TablePtr leftInput = dynamic_pointer_cast<Table>(leftData);
        if (leftInput == nullptr) {
            SQL_LOG(ERROR, "invalid input table");
            return navi::EC_ABORT;
        }
        if (!doCompute(runContext, leftInput)) {
            SQL_LOG(ERROR, "do compute failed");
            return navi::EC_ABORT;
        }
    }
    uint64_t endTime = TimeUtility::currentTime();
    incTotalTime(endTime - beginTime);
    if (leftEof) {
        runContext.setEof();
        SQL_LOG(DEBUG, "lookup join info: [%s]", _joinInfo.ShortDebugString().c_str());
    }
    return navi::EC_NONE;
}

bool LookupJoinKernel::doCompute(navi::KernelComputeContext &runContext,
                                    TablePtr leftTable)
{
    uint64_t beginTime = TimeUtility::currentTime();
    if (!lookupJoin(leftTable)) {
        return false;
    }
    uint64_t afterLookup = TimeUtility::currentTime();
    incJoinTime(afterLookup - beginTime);
    TablePtr outputTable(new Table(_memoryPoolResource->getPool()));
    if (!initJoinedTable(leftTable, _rightTable, outputTable)) {
        return false;
    }
    uint64_t afterInitTable = TimeUtility::currentTime();
    incInitTableTime(afterInitTable - afterLookup);
    if (!evaluateJoinedTable(leftTable, _rightTable, outputTable)) {
        return false;
    }
    uint64_t afterEvaluate = TimeUtility::currentTime();
    incEvaluateTime(afterEvaluate - afterInitTable);

    SQL_LOG(DEBUG, " join table success, joined table col[%zu] row[%zu]",
            outputTable->getColumnCount(), outputTable->getRowCount());
    SQL_LOG(DEBUG, " join output table: [%s]",
                    TableUtil::toString(outputTable, 10).c_str());
    navi::PortIndex outPort(0, navi::INVALID_INDEX);
    runContext.setOutput(outPort, outputTable);
    _rightTable->clearRows();
    return true;
}

bool LookupJoinKernel::lookupJoin(TablePtr leftTable) {
    const auto &leftJoinField = _joinField.begin()->first;
    ColumnPtr joinColumn = leftTable->getColumn(leftJoinField);
    if (joinColumn == nullptr) {
        SQL_LOG(ERROR, "invalid join column name [%s]", leftJoinField.c_str());
        return false;
    }
    auto schema = joinColumn->getColumnSchema();
    if (schema == nullptr) {
        SQL_LOG(ERROR, "get left join column [%s] schema failed",
                        leftJoinField.c_str());
        return false;
    }
    auto vt = schema->getType();
    bool isMulti = vt.isMultiValue();
    size_t rowCount = leftTable->getRowCount();
    if (!isMulti) {
        SQL_LOG(DEBUG, "make single join");
        return singleJoin(joinColumn, vt, rowCount);
    } else {
        SQL_LOG(DEBUG, "make multi join");
        return multiJoin(joinColumn, vt, rowCount);
    }
}


END_HA3_NAMESPACE(sql);
