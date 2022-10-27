#include <ha3/sql/ops/join/JoinBase.h>
#include <ha3/sql/ops/util/KernelUtil.h>
#include <ha3/sql/util/ValueTypeSwitch.h>
#include <ha3/sql/data/TableUtil.h>
#include <ha3/sql/ops/condition/ExprUtil.h>

using namespace std;
using namespace autil;

BEGIN_HA3_NAMESPACE(sql);
HA3_LOG_SETUP(sql, JoinBase);

JoinBase::JoinBase(const JoinBaseParam &joinBaseParam)
    : _joinInfo(joinBaseParam._joinInfo)
    , _poolPtr(joinBaseParam._poolPtr)
    , _pool(joinBaseParam._pool)
    , _calcTable(joinBaseParam._calcTable)
{
    getFieldPairsFromInput(joinBaseParam._leftInputFields,
                           joinBaseParam._rightInputFields,
                           joinBaseParam._outputFields);
}


void JoinBase::getFieldPairsFromInput(const std::vector<std::string> &leftInputFields,
                                      const std::vector<std::string> &rightInputFields,
                                      const std::vector<std::string> &outputFields)
{
    for (size_t i = 0; i < outputFields.size(); i++) {
        if (i < leftInputFields.size()) {
            _leftFields.emplace_back(outputFields[i], leftInputFields[i]);
        } else {
            size_t offset = i - leftInputFields.size();
            _rightFields.emplace_back(outputFields[i], rightInputFields[offset]);
        }
    }
}

bool JoinBase::initJoinedTable(const TablePtr &leftTable,
                               const TablePtr &rightTable,
                               TablePtr &outputTable)
{
    if (!appendColumns(_leftFields, leftTable, outputTable)) {
        SQL_LOG(ERROR, "append columns for left table failed");
        return false;
    }
    if (!appendColumns(_rightFields, rightTable, outputTable)) {
        SQL_LOG(ERROR, "append columns for right table failed");
        return false;
    }
    outputTable->endGroup();
    return true;
}

bool JoinBase::getColumnInfo(const TablePtr &table,
                             const string &field,
                             ColumnPtr &tableColumn,
                             ValueType &vt) const
{
    tableColumn = table->getColumn(field);
    if (tableColumn == nullptr) {
        SQL_LOG(ERROR, "column [%s] not found in table", field.c_str());
        return false;
    }
    auto schema = tableColumn->getColumnSchema();
    if (schema == nullptr) {
        SQL_LOG(ERROR, "get column [%s] schema failed", field.c_str());
        return false;
    }
    vt = schema->getType();
    return true;
}

bool JoinBase::appendColumns(const vector<pair<string, string>> &fields,
                             const TablePtr &inputTable,
                             TablePtr &outputTable) const
{
    for (const auto &fieldPair : fields) {
        const string &outputField = fieldPair.first;
        const string &inputField = fieldPair.second;
        ColumnPtr tableColumn = {};
        ValueType vt;
        if (!getColumnInfo(inputTable, inputField, tableColumn, vt)) {
            SQL_LOG(ERROR, "get column info failed");
            return false;
        }
        auto column = outputTable->declareColumn(outputField, vt, false);
        if (column == NULL) {
            SQL_LOG(ERROR, "declare column [%s] failed", outputField.c_str());
            return false;
        }
    }
    outputTable->endGroup();
    return true;
}

bool JoinBase::evaluateJoinedTable(const std::vector<size_t> &leftTableIndexes,
                                   const std::vector<size_t> &rightTableIndexes,
                                   const TablePtr &leftTable,
                                   const TablePtr &rightTable,
                                   TablePtr &outputTable)
{
    if (leftTableIndexes.size() != rightTableIndexes.size()) {
        SQL_LOG(ERROR, "tableA size[%zu] not match tableB size[%zu]",
                leftTableIndexes.size(), rightTableIndexes.size());
        return false;
    }

    JoinInfoCollector::incJoinCount(_joinInfo, leftTableIndexes.size());
    size_t joinIndexStart = outputTable->getRowCount();
    outputTable->batchAllocateRow(leftTableIndexes.size());

    if (!evaluateColumns(_leftFields, leftTableIndexes, leftTable, joinIndexStart, outputTable)) {
        SQL_LOG(ERROR, "evaluate columns for left table failed");
        return false;
    }
    if (!evaluateColumns(_rightFields, rightTableIndexes, rightTable, joinIndexStart, outputTable)) {
        SQL_LOG(ERROR, "evaluate columns for right table failed");
        return false;
    }
    return true;
}

bool JoinBase::evaluateColumns(const vector<pair<string, string>> &fields,
                               const vector<size_t> &rowIndexes,
                               const TablePtr &inputTable,
                               size_t joinIndexStart,
                               TablePtr &outputTable)
{
    for (const auto &fieldPair : fields) {
        const string &outputField = fieldPair.first;
        const string &inputField = fieldPair.second;
        size_t joinIndex = joinIndexStart;
        ColumnPtr tableColumn = {};
        ValueType vt;
        if (!getColumnInfo(inputTable, inputField, tableColumn, vt)) {
            SQL_LOG(ERROR, "get column [%s] info failed", inputField.c_str());
            return false;
        }
        auto newColumn = outputTable->getColumn(outputField);
        if (newColumn == nullptr) {
            SQL_LOG(ERROR, "get output table column [%s] failed", outputField.c_str());
            return false;
        }
        auto func = [&](auto a) {
            typedef typename decltype(a)::value_type T;
            auto columnData = tableColumn->getColumnData<T>();
            auto newColumnData = newColumn->getColumnData<T>();
            for (auto rowIndex : rowIndexes) {
                newColumnData->set(joinIndex, columnData->get(rowIndex));
                joinIndex++;
            }
            return true;
        };

        if (!ValueTypeSwitch::switchType(vt, func, func)) {
            SQL_LOG(ERROR, "evaluate columns failed");
            return false;
        }
    }
    return true;
}

bool JoinBase::evaluateEmptyColumns(const vector<pair<string, string>> &fields,
                                    size_t joinIndexStart,
                                    TablePtr &outputTable)
{
    for (const auto &fieldPair : fields) {
        const string &outputField = fieldPair.first;
        size_t joinIndex = joinIndexStart;
        ColumnPtr outputColumn = {};
        ValueType vt;
        if (!getColumnInfo(outputTable, outputField, outputColumn, vt)) {
            SQL_LOG(ERROR, "get column [%s] info failed", outputField.c_str());
            return false;
        }

        auto func = [&](auto a) {
            typedef typename decltype(a)::value_type T;
            const std::string &type = ExprUtil::transVariableTypeToSqlType(vt.getBuiltinType());
            auto iter = _defaultValue.find(type);
            T val;
            InitializeIfNeeded<T>()(val);
            if (!type.empty() && iter != _defaultValue.end()) {
                if (!StringUtil::fromString<T>(iter->second, val)) {
                    SQL_LOG(ERROR, "field [%s] type [%s] cast default value [%s] failed",
                            outputField.c_str(), type.c_str(), iter->second.c_str());
                    return false;
                }
            }
            auto outputColumnData = outputColumn->getColumnData<T>();
            for (size_t i = joinIndex; i < outputTable->getRowCount(); ++i) {
                outputColumnData->set(i, val);
            }
            return true;
        };
        auto strFunc = [&]() {
            const std::string &type = ExprUtil::transVariableTypeToSqlType(vt.getBuiltinType());
            auto iter = _defaultValue.find(type);

            if (!type.empty() && iter != _defaultValue.end()) {
                MultiChar val(MultiValueCreator::createMultiValueBuffer(
                                iter->second.data(), iter->second.size(), _pool));
                auto outputColumnData = outputColumn->getColumnData<MultiChar>();
                for (size_t i = joinIndex; i < outputTable->getRowCount(); ++i) {
                    outputColumnData->set(i, val);
                }
            }
            return true;
        };
        auto multiFunc = [&](auto a) {
            return true;
        };
        auto multiStrFunc = [&]() {
            return true;
        };
        if (!ValueTypeSwitch::switchType(vt, func, multiFunc, strFunc, multiStrFunc)) {
            SQL_LOG(ERROR, "evaluate empty columns failed");
            return false;
        }
    }
    return true;
}

END_HA3_NAMESPACE(sql);
