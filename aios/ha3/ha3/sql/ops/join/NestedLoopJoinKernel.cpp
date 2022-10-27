#include <ha3/sql/ops/join/NestedLoopJoinKernel.h>
#include <ha3/sql/ops/util/KernelRegister.h>
#include <ha3/sql/ops/util/KernelUtil.h>
#include <ha3/sql/ops/condition/ConditionParser.h>
#include <ha3/sql/resource/SqlQueryResource.h>
#include <ha3/sql/data/TableUtil.h>

using namespace std;
using namespace autil;
using namespace matchdoc;
using namespace autil;

BEGIN_HA3_NAMESPACE(sql);

const size_t NestedLoopJoinKernel::DEFAULT_BUFFER_LIMIT_SIZE = 1024 * 1024;

NestedLoopJoinKernel::NestedLoopJoinKernel()
    : _bufferLimitSize(DEFAULT_BUFFER_LIMIT_SIZE)
    , _fullTableCreated(false)
    , _leftFullTable(true)
    , _leftEof(false)
    , _rightEof(false)
{
}

NestedLoopJoinKernel::~NestedLoopJoinKernel() {
    reportMetrics();
}

const navi::KernelDef *NestedLoopJoinKernel::getDef() const {
    auto def = new navi::KernelDef();
    def->set_kernel_name("NestedLoopJoinKernel");
    KERNEL_REGISTER_ADD_INPUT(def, "input0");
    KERNEL_REGISTER_ADD_INPUT(def, "input1");
    KERNEL_REGISTER_ADD_OUTPUT(def, "output0");
    return def;
}

bool NestedLoopJoinKernel::config(navi::KernelConfigContext &ctx) {
    if (!JoinKernelBase::config(ctx)) {
        return false;
    }
    if (_conditionJson.empty()) {
        SQL_LOG(ERROR, "nest loop join condition is empty");
        return false;
    }
    ctx.getJsonAttrs().Jsonize("buffer_limit_size", _bufferLimitSize, _bufferLimitSize);
    return true;
}

navi::ErrorCode NestedLoopJoinKernel::compute(navi::KernelComputeContext &runContext) {
    JoinInfoCollector::incComputeTimes(&_joinInfo);
    uint64_t beginTime = TimeUtility::currentTime();
    if (!getInput(runContext, 0, _leftBuffer, _leftEof)) {
        SQL_LOG(ERROR, "get left input failed");
        return navi::EC_ABORT;
    }
    if (!getInput(runContext, 1, _rightBuffer, _rightEof)) {
        SQL_LOG(ERROR, "get right input failed");
        return navi::EC_ABORT;
    }
    if ((_leftEof && _leftBuffer == nullptr) || (_rightEof && _rightBuffer == nullptr)) {
        runContext.setEof();
        return navi::EC_NONE;
    }
    if (!_fullTableCreated && !waitFullTable()) {
        SQL_LOG(ERROR, "wait full table failed");
        return navi::EC_ABORT;
    }
    if (_fullTableCreated) {
        TablePtr outputTable = nullptr;
        if (!doCompute(outputTable)) {
            return navi::EC_ABORT;
        }
        navi::PortIndex outPort(0, navi::INVALID_INDEX);
        runContext.setOutput(outPort, outputTable);
    }
    uint64_t endTime = TimeUtility::currentTime();
    JoinInfoCollector::incTotalTime(&_joinInfo, endTime - beginTime);
    auto largeTable = _leftFullTable ? _rightBuffer : _leftBuffer;
    if (_leftEof && _rightEof && largeTable->getRowCount() == 0) {
        runContext.setEof();
        if (_sqlSearchInfoCollector) {
            _sqlSearchInfoCollector->addJoinInfo(_joinInfo);
        }
        SQL_LOG(DEBUG, "join info: [%s]", _joinInfo.ShortDebugString().c_str());
    }

    return navi::EC_NONE;
}

bool NestedLoopJoinKernel::doCompute(TablePtr &outputTable) {
    auto largeTable = _leftFullTable ? _rightBuffer : _leftBuffer;
    size_t beforeJoinedRowCount = largeTable->getRowCount();
    while (true) {
        size_t joinedRowCount = 0;
        if (!joinTable(joinedRowCount)) {
            SQL_LOG(ERROR, "join table failed");
            return false;
        }
        const std::vector<size_t> &leftTableIndexs =
            _leftFullTable ? _tableBIndexes : _tableAIndexes;
        const std::vector<size_t> &rightTableIndexs =
            _leftFullTable ? _tableAIndexes : _tableBIndexes;
        size_t leftTableSize = _leftFullTable ? _leftBuffer->getRowCount() : joinedRowCount;
        if (!_joinPtr->generateResultTable(
                        leftTableIndexs, rightTableIndexs,
                        _leftBuffer, _rightBuffer,
                        leftTableSize, outputTable))
        {
            SQL_LOG(ERROR, "generate result table failed");
            return false;
        }
        _tableAIndexes.clear();
        _tableBIndexes.clear();

        if (!outputTable) {
            SQL_LOG(ERROR, "generate join result table failed");
            return false;
        }
        outputTable->deleteRows();

        if (!_leftFullTable || (_rightEof && largeTable->getRowCount() == joinedRowCount)) {
            if (!_joinPtr->finish(_leftBuffer, leftTableSize, outputTable))
            {
                SQL_LOG(ERROR, "left buffer finish fill table failed");
                return false;
            }
        }

        if (joinedRowCount > 0) {
            largeTable->clearFrontRows(joinedRowCount);
        } else if (_shouldClearTable) {
            largeTable->clearRows();
        }
        if (largeTable->getRowCount() == 0 || outputTable->getRowCount() >= _batchSize) {
            break;
        }
    }

    SQL_LOG(DEBUG, "joined row count [%zu], joined table remaining count [%zu],"
            " left eof [%d], right eof [%d]",
            beforeJoinedRowCount - largeTable->getRowCount(),
            largeTable->getRowCount(), _leftEof, _rightEof);
    if (_joinInfo.totalhashtime() < 5) {
        SQL_LOG(DEBUG, "nest loop join output table: [%s]",
                TableUtil::toString(outputTable, 10).c_str());
    }
    return true;
}

bool NestedLoopJoinKernel::getInput(navi::KernelComputeContext &runContext,
                                  size_t index, TablePtr &buffer, bool &eof)
{
    if (!buffer || buffer->getRowCount() < _bufferLimitSize) {
        navi::PortIndex port(index, navi::INVALID_INDEX);
        navi::DataPtr data;
        runContext.getInput(port, data, eof);
        if (data != nullptr) {
            auto input = dynamic_pointer_cast<Table>(data);
            if (input == nullptr) {
                SQL_LOG(ERROR, "invalid input table");
                return false;
            }
            if (!buffer) {
                buffer = input;
            } else {
                if (!buffer->merge(input)) {
                    SQL_LOG(ERROR, "merge input failed");
                    return false;
                }
            }
        }
    }
    return true;
}

bool NestedLoopJoinKernel::waitFullTable() {
    if (_leftBuffer && _leftBuffer->getRowCount() > _bufferLimitSize &&
        _rightBuffer && _rightBuffer->getRowCount() > _bufferLimitSize)
    {
        SQL_LOG(ERROR, "input buffers exceed limit [%lu]", _bufferLimitSize);
        return false;
    }
    // todo optimize
    if (_leftEof)
    {
        _leftFullTable = true;
        _fullTableCreated = true;
        SQL_LOG(DEBUG, "left buffer is full table."
                " left buffer size[%zu], right buffer size[%zu]",
                _leftBuffer->getRowCount(), _rightBuffer->getRowCount());
    } else if (_rightEof) {
        _leftFullTable = false;
        _fullTableCreated = true;
        SQL_LOG(DEBUG, "right buffer is full table."
                " left buffer size[%zu], right buffer size[%zu]",
                _leftBuffer->getRowCount(), _rightBuffer->getRowCount());
    }
    return true;
}

bool NestedLoopJoinKernel::joinTable(size_t &joinedRowCount) {
    uint64_t beginJoin = TimeUtility::currentTime();
    auto largeTable = _leftFullTable ? _rightBuffer : _leftBuffer;
    auto fullTable  = _leftFullTable ? _leftBuffer : _rightBuffer;
    size_t joinedCount = 0;
    for (size_t i = 0; i < largeTable->getRowCount(); i++) {
        if (joinedCount >= _batchSize) {
            SQL_LOG(DEBUG, "joined count[%zu] over batch size[%zu]",
                            joinedCount, _batchSize);
            break;
        }
        for (size_t j = 0; j < fullTable->getRowCount(); j++) {
            joinRow(i, j);
        }
        joinedCount += fullTable->getRowCount();
        joinedRowCount = i + 1;
    }
    uint64_t afterJoin = TimeUtility::currentTime();
    JoinInfoCollector::incJoinTime(&_joinInfo, afterJoin - beginJoin);
    return true;
}


REGISTER_KERNEL(NestedLoopJoinKernel);

END_HA3_NAMESPACE(sql);
