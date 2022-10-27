#include <ha3/sql/ops/agg/builtin/CountAggFunc.h>

using namespace std;
using namespace matchdoc;

BEGIN_HA3_NAMESPACE(sql);
HA3_LOG_SETUP(sql, CountAggFunc);
HA3_LOG_SETUP(sql, CountAggFuncCreator);

// local
bool CountAggFunc::initCollectInput(const TablePtr &inputTable) {
    return true;
};

bool CountAggFunc::initAccumulatorOutput(const TablePtr &outputTable) {
    assert(_outputFields.size() == 1);
    _countColumn = TableUtil::declareAndGetColumnData<uint64_t>(outputTable, _outputFields[0], false);
    if (_countColumn == nullptr) {
        return false;
    }
    return true;
}

bool CountAggFunc::collect(Row inputRow, Accumulator *acc) {
    CountAccumulator *countAcc = static_cast<CountAccumulator *>(acc);
    ++countAcc->value;
    return true;
}

bool CountAggFunc::outputAccumulator(Accumulator *acc, Row outputRow) const {
    CountAccumulator *countAcc = static_cast<CountAccumulator *>(acc);
    _countColumn->set(outputRow, countAcc->value);
    return true;
}

bool CountAggFunc::initMergeInput(const TablePtr &inputTable) {
    assert(_inputFields.size() == 1);
    _inputColumn = TableUtil::getColumnData<uint64_t>(inputTable, _inputFields[0]);
    if (_inputColumn == nullptr) {
        return false;
    }
    return true;
}

bool CountAggFunc::initResultOutput(const TablePtr &outputTable) {
    return initAccumulatorOutput(outputTable);
}

bool CountAggFunc::merge(Row inputRow, Accumulator *acc) {
    CountAccumulator *countAcc = static_cast<CountAccumulator *>(acc);
    countAcc->value += _inputColumn->get(inputRow);
    return true;
}

bool CountAggFunc::outputResult(Accumulator *acc, Row outputRow) const {
    return outputAccumulator(acc, outputRow);
}

AggFunc *CountAggFuncCreator::createLocalFunction(
        const vector<ValueType> &inputTypes,
        const vector<string> &inputFields,
        const vector<string> &outputFields)
{
    if (inputFields.size() > 1) {
        SQL_LOG(WARN, "impossible input length, expect 0 or 1, actual %lu", inputFields.size());
        return nullptr;
    }
    if (outputFields.size() != 1) {
        SQL_LOG(WARN, "impossible output length, expect 1, actual %lu", outputFields.size());
        return nullptr;
    }
    return new CountAggFunc(inputFields, outputFields, true);
}

AggFunc *CountAggFuncCreator::createGlobalFunction(
        const vector<ValueType> &inputTypes,
        const vector<string> &inputFields,
        const vector<string> &outputFields)
{
    if (inputFields.size() != 1) {
        SQL_LOG(WARN, "impossible input length, expect 1, actual %lu", inputFields.size());
        return nullptr;
    }
    if (outputFields.size() != 1) {
        SQL_LOG(WARN, "impossible output length, expect 1, actual %lu", outputFields.size());
        return nullptr;
    }
    if (inputTypes[0].getTypeIgnoreConstruct() !=
        ValueTypeHelper<uint64_t>::getValueType().getTypeIgnoreConstruct())
    {
        SQL_LOG(WARN, "impossible input type, expect uint64, actual %s",
                TableUtil::valueTypeToString(inputTypes[0]).c_str());
        return nullptr;
    }
    return new CountAggFunc(inputFields, outputFields, false);
}

END_HA3_NAMESPACE(sql);
