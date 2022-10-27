#include <ha3/sql/ops/join/HashJoinKernel.h>
#include <ha3/sql/ops/join/HashJoinConditionVisitor.h>
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

const size_t HashJoinKernel::DEFAULT_BUFFER_LIMIT_SIZE = 1024 * 1024;

HashJoinKernel::HashJoinKernel()
    : _bufferLimitSize(DEFAULT_BUFFER_LIMIT_SIZE)
    , _hashMapCreated(false)
    , _hashLeftTable(true)
    , _leftEof(false)
    , _rightEof(false)
{
}

HashJoinKernel::~HashJoinKernel() {
    reportMetrics();
}

const navi::KernelDef *HashJoinKernel::getDef() const {
    auto def = new navi::KernelDef();
    def->set_kernel_name("HashJoinKernel");
    KERNEL_REGISTER_ADD_INPUT(def, "input0");
    KERNEL_REGISTER_ADD_INPUT(def, "input1");
    KERNEL_REGISTER_ADD_OUTPUT(def, "output0");
    return def;
}

bool HashJoinKernel::config(navi::KernelConfigContext &ctx) {
    if (!JoinKernelBase::config(ctx)) {
        return false;
    }
    if (_conditionJson.empty()) {
        SQL_LOG(ERROR, "hash join condition is empty");
        return false;
    }
    ctx.getJsonAttrs().Jsonize("buffer_limit_size", _bufferLimitSize, _bufferLimitSize);
    return true;
}

navi::ErrorCode HashJoinKernel::compute(navi::KernelComputeContext &runContext) {
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
    if (!_hashMapCreated && !tryCreateHashMap()) {
        SQL_LOG(ERROR, "create hash map failed");
        return navi::EC_ABORT;
    }
    if (_hashMapCreated) {
        TablePtr outputTable = nullptr;
        if (!doCompute(outputTable)) {
            return navi::EC_ABORT;
        }
        navi::PortIndex outPort(0, navi::INVALID_INDEX);
        runContext.setOutput(outPort, outputTable);
    }
    uint64_t endTime = TimeUtility::currentTime();
    JoinInfoCollector::incTotalTime(&_joinInfo, endTime - beginTime);
    auto largeTable = _hashLeftTable ? _rightBuffer : _leftBuffer;
    if (_leftEof && _rightEof && largeTable->getRowCount() == 0) {
        runContext.setEof();
        if (_sqlSearchInfoCollector) {
            _sqlSearchInfoCollector->addJoinInfo(_joinInfo);
        }
        SQL_LOG(DEBUG, "join info: [%s]", _joinInfo.ShortDebugString().c_str());
    }

    return navi::EC_NONE;
}

bool HashJoinKernel::doCompute(TablePtr &outputTable) {
    size_t joinedRowCount = 0;
    if (!joinTable(joinedRowCount)) {
        SQL_LOG(ERROR, "join table failed");
        return false;
    }
    auto largeTable = _hashLeftTable ? _rightBuffer : _leftBuffer;
    if (_shouldClearTable) {
        joinedRowCount = largeTable->getRowCount();
    }
    const std::vector<size_t> &leftTableIndexs = _hashLeftTable ? _tableAIndexes : _tableBIndexes;
    const std::vector<size_t> &rightTableIndexs = _hashLeftTable ? _tableBIndexes : _tableAIndexes;
    size_t leftTableSize = _hashLeftTable ? _leftBuffer->getRowCount() : joinedRowCount;
    if (!_joinPtr->generateResultTable(leftTableIndexs, rightTableIndexs,
                    _leftBuffer, _rightBuffer, leftTableSize, outputTable))
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

    if (!_hashLeftTable || (_rightEof && largeTable->getRowCount() == joinedRowCount)) {
        if (!_joinPtr->finish(_leftBuffer, leftTableSize, outputTable)) {
            SQL_LOG(ERROR, "left buffer finish fill table failed");
            return false;
        }
    }
    if (joinedRowCount > 0) {
        largeTable->clearFrontRows(joinedRowCount);
    }
    SQL_LOG(DEBUG, "joined row count [%zu], joined table remaining count [%zu],"
            " left eof [%d], right eof [%d]",
            joinedRowCount, largeTable->getRowCount(), _leftEof, _rightEof);
    if (_joinInfo.totalhashtime() < 5) {
        SQL_LOG(DEBUG, "hash join output table: [%s]",
                TableUtil::toString(outputTable, 10).c_str());
    }
    return true;
}

bool HashJoinKernel::getInput(navi::KernelComputeContext &runContext,
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

bool HashJoinKernel::tryCreateHashMap() {
    if (_leftBuffer && _leftBuffer->getRowCount() > _bufferLimitSize &&
        _rightBuffer && _rightBuffer->getRowCount() > _bufferLimitSize)
    {
        SQL_LOG(ERROR, "input buffers exceed limit, cannot make hash join");
        return false;
    }
    // todo optimize
    if (_leftEof && _rightBuffer &&
        _leftBuffer->getRowCount() <= _rightBuffer->getRowCount())
    {
        _hashLeftTable = true;
        if (!createHashMap(_leftBuffer, 0, _leftBuffer->getRowCount(),  _hashLeftTable)) {
            SQL_LOG(ERROR, "create hash table with left buffer failed.");
            return false;
        }
        _hashMapCreated = true;
        SQL_LOG(TRACE1, "create hash table with left buffer."
                " left buffer size[%zu], right buffer size[%zu], hash map size[%zu]",
                _leftBuffer->getRowCount(), _rightBuffer->getRowCount(),
                _hashJoinMap.size());
    } else if (_rightEof && _leftBuffer &&
               _rightBuffer->getRowCount() <= _leftBuffer->getRowCount())
    {
        _hashLeftTable = false;
        if (!createHashMap(_rightBuffer, 0, _rightBuffer->getRowCount(),  _hashLeftTable)) {
            SQL_LOG(ERROR, "create hash table with right buffer failed.");
            return false;
        }
        _hashMapCreated = true;
        SQL_LOG(TRACE1, "create hash table with right buffer."
                " left buffer size[%zu], right buffer size[%zu], hash map size[%zu]",
                _leftBuffer->getRowCount(), _rightBuffer->getRowCount(),
                _hashJoinMap.size());
    }
    return true;
}

bool HashJoinKernel::joinTable(size_t &joinedRowCount) {
    uint64_t beginJoin = TimeUtility::currentTime();
    auto largeTable = _hashLeftTable ? _rightBuffer : _leftBuffer;
    const auto &joinColumns = _hashLeftTable ? _rightJoinColumns : _leftJoinColumns;
    HashValues largeTableValues;
    if (!getHashValues(largeTable, 0, largeTable->getRowCount(),
                       joinColumns, largeTableValues))
    {
        return false;
    }
    if (_hashLeftTable) {
        JoinInfoCollector::incRightHashCount(&_joinInfo, largeTableValues.size());
    } else {
        JoinInfoCollector::incLeftHashCount(&_joinInfo, largeTableValues.size());
    }
    uint64_t afterHash = TimeUtility::currentTime();
    JoinInfoCollector::incHashTime(&_joinInfo, afterHash - beginJoin);
    joinedRowCount = makeHashJoin(largeTableValues);
    uint64_t afterJoin = TimeUtility::currentTime();
    JoinInfoCollector::incJoinTime(&_joinInfo, afterJoin - afterHash);
    return true;
}

size_t HashJoinKernel::makeHashJoin(const HashValues &values) {
    if (values.empty()) {
        return 0;
    }
    size_t joinedCount = 0;
    size_t oriRow = values[0].first;
    reserveJoinRow(values.size());
    for (const auto &valuePair : values) {
        auto &largeRow = valuePair.first;
        // multi field joined same row
        if (largeRow > oriRow && joinedCount >= _batchSize) {
            SQL_LOG(TRACE1, "joined count[%zu] over batch size[%zu], used large row[%zu]",
                            joinedCount, _batchSize, largeRow);
            return largeRow;
        }
        auto iter = _hashJoinMap.find(valuePair.second);
        if (iter != _hashJoinMap.end()) {
            auto &joinedRows = iter->second;
            for (auto row : joinedRows) {
                joinRow(row, largeRow);
            }
            joinedCount += joinedRows.size();
        }
        oriRow = largeRow;
    }
    SQL_LOG(TRACE1, "joined count[%zu], used large row[%zu]", joinedCount, oriRow + 1);
    return oriRow + 1;
}

REGISTER_KERNEL(HashJoinKernel);

END_HA3_NAMESPACE(sql);
