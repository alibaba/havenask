#include <ha3/sql/ops/join/LookupJoinKernel.h>
#include <ha3/sql/ops/util/KernelRegister.h>
#include <ha3/common/AndQuery.h>
#include <ha3/common/OrQuery.h>
#include <ha3/common/TermQuery.h>
#include <ha3/common/NumberQuery.h>
#include <ha3/common/MultiTermQuery.h>
#include <ha3/sql/ops/scan/NormalScan.h>
#include <ha3/sql/ops/scan/SummaryScan.h>
#include <autil/TimeUtility.h>
#include <autil/StringUtil.h>
#include <ha3/sql/util/ValueTypeSwitch.h>
#include <unordered_map>
#include <unordered_set>

using namespace std;
using namespace autil;
using namespace matchdoc;
using namespace autil;
USE_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(sql);

static const size_t LOOKUP_BATCH_SIZE = 512;

LookupJoinKernel::LookupJoinKernel()
    : _lastScanEof(true)
    , _inputEof(false)
    , _lookupBatchSize(LOOKUP_BATCH_SIZE)
    , _lookupTurncateThreshold(0)
    , _hasJoinedCount(0)
{
}

LookupJoinKernel::~LookupJoinKernel() {
    reportMetrics();
    _scanBase.reset();
}

const navi::KernelDef *LookupJoinKernel::getDef() const {
    auto def = new navi::KernelDef();
    def->set_kernel_name("LookupJoinKernel");
    KERNEL_REGISTER_ADD_INPUT(def, "input0");
    KERNEL_REGISTER_ADD_OUTPUT(def, "output0");
    return def;
}

bool LookupJoinKernel::config(navi::KernelConfigContext &ctx) {
    auto &json = ctx.getJsonAttrs();
    if (!JoinKernelBase::config(ctx)) {
        return false;
    }
    json.Jsonize("lookup_batch_size", _lookupBatchSize, _lookupBatchSize);
    if (_lookupBatchSize == 0) {
        _lookupBatchSize = LOOKUP_BATCH_SIZE;
    }
    autil::legacy::Jsonizable::JsonWrapper wrapper;
    if (!KernelUtil::toJsonWrapper(json, "build_node", wrapper)) {
        return false;
    }
    if (!_initParam.initFromJson(wrapper)) {
        return false;
    }

    KernelUtil::stripName(_initParam.outputFields);
    KernelUtil::stripName(_initParam.indexInfos);
    KernelUtil::stripName(_initParam.usedFields);
    KernelUtil::stripName(_initParam.hashFields);
    patchLookupHintInfo(_hashHints);
    return true;
}

navi::ErrorCode LookupJoinKernel::init(navi::KernelInitContext& context) {
    navi::ErrorCode baseEc = JoinKernelBase::init(context);
    if (baseEc != navi::EC_NONE) {
        return baseEc;
    }
    if (_leftIsBuild && (_joinType == SQL_LEFT_JOIN_TYPE || _joinType == SQL_ANTI_JOIN_TYPE)) {
        SQL_LOG(ERROR, "leftIsBuild is conflict with joinType[%s]!", _joinType.c_str());
        return navi::EC_ABORT;
    }
    _initParam.opName = getKernelName();
    _initParam.nodeName = getNodeName();
    _initParam.memoryPoolResource = _memoryPoolResource;
    _initParam.bizResource = context.getResource<SqlBizResource>("SqlBizResource");
    _initParam.queryResource = context.getResource<SqlQueryResource>("SqlQueryResource");
    if (_initParam.tableType == SCAN_NORMAL_TABLE_TYPE) {
        _scanBase.reset(new NormalScan());
    } else if (_initParam.tableType == SCAN_SUMMARY_TABLE_TYPE) {
        _scanBase.reset(new SummaryScan(false));
    } else {
        SQL_LOG(ERROR, "not support table type [%s].", _initParam.tableType.c_str());
    }
    if (_scanBase == nullptr || !_scanBase->init(_initParam)) {
        SQL_LOG(ERROR, "init scan kernel failed, table [%s].",
                        _initParam.tableName.c_str());
        return navi::EC_ABORT;
    }
    SQL_LOG(DEBUG, "batch size [%ld] lookup batch size [%ld], lookup threshold [%ld].",
            _batchSize, _lookupBatchSize, _lookupTurncateThreshold);

    return navi::EC_NONE;
}

navi::ErrorCode LookupJoinKernel::compute(navi::KernelComputeContext &runContext) {
    JoinInfoCollector::incComputeTimes(&_joinInfo);
    uint64_t beginTime = TimeUtility::currentTime();
    if (_inputTable == nullptr || _inputTable->getRowCount() == 0) {
        navi::PortIndex portIndex(0, navi::INVALID_INDEX);
        navi::DataPtr data;
        runContext.getInput(portIndex, data, _inputEof);
        if (data != nullptr) {
            _inputTable = dynamic_pointer_cast<Table>(data);
            if (_inputTable == nullptr) {
                SQL_LOG(WARN, "input is not table");
                return navi::EC_ABORT;
            }
        }
    }
    if (_inputTable) {
        TablePtr outputTable;
        if (!doLookupJoin(outputTable) || outputTable == nullptr) {
            SQL_LOG(ERROR, "do lookup join failed");
            return navi::EC_ABORT;
        }
        if (_joinInfo.totalhashtime() < 5) {
            SQL_LOG(TRACE3, "lookup join output table: [%s]",
                    TableUtil::toString(outputTable, 10).c_str());
        }
        if (canTurncate()) {
            SQL_LOG(INFO, "join trigger turncate [%ld] threshold [%ld].",
                    _hasJoinedCount, _lookupTurncateThreshold);
            navi::PortIndex outPort(0, navi::INVALID_INDEX);
            runContext.setOutput(outPort, outputTable, true);
            uint64_t endTime = TimeUtility::currentTime();
            JoinInfoCollector::incTotalTime(&_joinInfo, endTime - beginTime);
            if (_sqlSearchInfoCollector) {
                _sqlSearchInfoCollector->addJoinInfo(_joinInfo);
            }
            SQL_LOG(DEBUG, "lookup join info: [%s]", _joinInfo.ShortDebugString().c_str());
            return navi::EC_NONE;
        } else {
            navi::PortIndex outPort(0, navi::INVALID_INDEX);
            runContext.setOutput(outPort, outputTable);
        }
    }
    uint64_t endTime = TimeUtility::currentTime();
    JoinInfoCollector::incTotalTime(&_joinInfo, endTime - beginTime);
    if (_inputEof && (_inputTable == nullptr || _inputTable->getRowCount() == 0)) {
        runContext.setEof();
        if (_sqlSearchInfoCollector) {
            _sqlSearchInfoCollector->addJoinInfo(_joinInfo);
        }
        SQL_LOG(DEBUG, "lookup join info: [%s]", _joinInfo.ShortDebugString().c_str());
    }
    return navi::EC_NONE;
}

void LookupJoinKernel::setEnableRowDeduplicate(bool enableRowDeduplicate) {
    _enableRowDeduplicate = enableRowDeduplicate;
}

bool LookupJoinKernel::doLookupJoin(TablePtr &outputTable)
{
    if (_inputTable == nullptr || _scanBase == nullptr) {
        SQL_LOG(WARN, "input table is empty.");
        return false;
    }
    bool isEmpty = true;
    StreamQueryPtr streamQuery;
    size_t rowOffset = 0;
    size_t rowCount = _inputTable->getRowCount();
    for (; rowOffset < rowCount; rowOffset += _lookupBatchSize) {
        size_t count = min(_lookupBatchSize, rowCount - rowOffset);
        if (_lastScanEof) {
            streamQuery = genFinalStreamQuery(_inputTable, rowOffset, count);
            if (streamQuery == nullptr) {
                SQL_LOG(INFO, "generate stream query failed, offset [%ld], table [%s]",
                        rowOffset, TableUtil::rowToString(_inputTable, rowOffset).c_str());
                continue;
            }
            if (!_scanBase->updateScanQuery(streamQuery)) {
                SQL_LOG(WARN, "update scan query failed, query [%s]", streamQuery->toString().c_str());
                return false;
            }
        }
        isEmpty = false;
        _lastScanEof = false;
        while (!_lastScanEof) {
            TablePtr streamOutput;
            bool eof = false;
            if (!_scanBase->batchScan(streamOutput, eof)) {
                return false;
            }
            if (streamOutput == nullptr) {
                SQL_LOG(WARN, "stream output is empty, query [%s]", streamQuery->toString().c_str());
                return false;
            }
            if (!joinTable(_inputTable, rowOffset, count, streamOutput, outputTable)) {
                return false;
            }
            if (_leftIsBuild &&
                !_joinPtr->finish(streamOutput, streamOutput->getRowCount(), outputTable))
            {
                SQL_LOG(ERROR, "query stream table finish fill table failed");
                return false;
            }
            _lastScanEof = eof;
            if (outputTable->getRowCount() >= _batchSize || canTurncate()) {
                size_t inputTableJoinedRowCount = _lastScanEof ? rowOffset + count : rowOffset;
                if (!_leftIsBuild &&
                    !_joinPtr->finish(_inputTable, inputTableJoinedRowCount, outputTable))
                {
                    SQL_LOG(ERROR, "input table finish fill table failed");
                    return false;
                }
                outputTable->deleteRows();
                _inputTable->clearFrontRows(inputTableJoinedRowCount);
                return true;
            }
        }

    }
    if (isEmpty) {
        if (!emptyJoin(_inputTable, outputTable)) {
            SQL_LOG(WARN, "empty join failed, table [%s]", TableUtil::toString(_inputTable, 5).c_str());
            return false;
        } else {
            _inputTable.reset();
            return true;
        }
    }
    if (!_leftIsBuild &&
        !_joinPtr->finish(_inputTable, _inputTable->getRowCount(), outputTable))
    {
        SQL_LOG(ERROR, "input table finish fill table failed");
        return false;
    }
    outputTable->deleteRows();
    _inputTable.reset();
    return true;
}

bool LookupJoinKernel::joinTable(const TablePtr &inputTable, size_t offset, size_t count,
                                 const TablePtr &streamOutput, TablePtr &outputTable)
{
    if (!joinTable(inputTable, offset, count, streamOutput)) {
        return false;
    }

    const vector<size_t> &leftTableIndexs = _tableAIndexes;
    const vector<size_t> &rightTableIndexs = _tableBIndexes;
    const auto &leftTable = _leftIsBuild ? streamOutput : inputTable;
    const auto &rightTable = _leftIsBuild ? inputTable : streamOutput;

    if (!_joinPtr->generateResultTable(leftTableIndexs, rightTableIndexs,
                    leftTable, rightTable, leftTable->getRowCount(), outputTable))
    {
        SQL_LOG(ERROR, "generate result table failed");
        return false;
    }
    _tableAIndexes.clear();
    _tableBIndexes.clear();

    return true;
}

bool LookupJoinKernel::joinTable(const TablePtr &inputTable, size_t offset, size_t count,
                                 const TablePtr &streamTable)
{
    _tableAIndexes.clear();
    _tableBIndexes.clear();
    if (streamTable->getRowCount() == 0) {
        return true;
    }
    if (!createHashMap(_inputTable, offset, count, !_leftIsBuild)) { // todo: once per scan eof
        SQL_LOG(ERROR, "create hash table with right buffer failed.");
        return false;
    }
    uint64_t beginJoin = TimeUtility::currentTime();
    const auto &joinColumns = _leftIsBuild ? _leftJoinColumns : _rightJoinColumns;
    HashValues hashValues;
    if (!getHashValues(streamTable, 0, streamTable->getRowCount(), joinColumns, hashValues)) {
        return false;
    }
    if (_leftIsBuild) {
        JoinInfoCollector::incLeftHashCount(&_joinInfo, hashValues.size());
    } else {
        JoinInfoCollector::incRightHashCount(&_joinInfo, hashValues.size());
    }
    uint64_t afterHash = TimeUtility::currentTime();
    JoinInfoCollector::incHashTime(&_joinInfo, afterHash - beginJoin);
    reserveJoinRow(hashValues.size());
    set<size_t> rowSet;
    for (const auto &valuePair : hashValues) {
        auto iter = _hashJoinMap.find(valuePair.second);
        if (iter != _hashJoinMap.end()) {
            auto &toJoinRows = iter->second;
            for (auto row : toJoinRows) {
                rowSet.insert(row);
                if (_leftIsBuild) {
                    joinRow(valuePair.first, row);
                } else {
                    joinRow(row, valuePair.first);
                }
            }
        }
    }
    _hasJoinedCount += rowSet.size();
    uint64_t afterJoin = TimeUtility::currentTime();
    JoinInfoCollector::incJoinTime(&_joinInfo, afterJoin - afterHash);
    return true;
}

bool LookupJoinKernel::emptyJoin(const TablePtr &inputTable, TablePtr &outputTable) {
    if (outputTable != nullptr) {
        return true;
    }
    if (!_scanBase->updateScanQuery(StreamQueryPtr())) {
        SQL_LOG(WARN, "empty stream scan failed.");
        return false;
    }
    TablePtr streamOutput;
    bool eof = false;
    if (!_scanBase->batchScan(streamOutput, eof)) {
        SQL_LOG(WARN, "empty stream scan failed.");
        return false;
    }
    if (streamOutput == nullptr) {
        SQL_LOG(WARN, "stream output is empty failed.");
        return false;
    }
    auto leftTable = _leftIsBuild ? streamOutput : inputTable;
    auto rightTable = _leftIsBuild ? inputTable : streamOutput;
    if (!_joinPtr->generateResultTable({}, {}, leftTable, rightTable,
                    leftTable->getRowCount(), outputTable))
    {
        SQL_LOG(WARN, "generate join table failed, left [%s] right[%s]",
                TableUtil::toString(streamOutput, 5).c_str(),
                TableUtil::toString(inputTable, 5).c_str());
        return false;
    }
    if (!_joinPtr->finish(leftTable, leftTable->getRowCount(), outputTable)) {
        SQL_LOG(WARN, "left table finish failed");
        return false;
    }
    return true;
}

StreamQueryPtr LookupJoinKernel::genFinalStreamQuery(const TablePtr &input, size_t offset, size_t count) {
    StreamQueryPtr streamQuery;
    if (_initParam.tableType == SCAN_KV_TABLE_TYPE || _initParam.tableType == SCAN_KKV_TABLE_TYPE) {
        vector<string> pks;
        if (genStreamKeys(input, offset, count, pks)) {
            streamQuery.reset(new StreamQuery());
            streamQuery->primaryKeys = pks;
        }
    } else {
        QueryPtr query;
        if (_initParam.tableType == SCAN_NORMAL_TABLE_TYPE) {
            query = genTableQuery(input, offset, count);
        } else {
            query = genStreamQuery(input, offset, count);
        }
        if (nullptr != query) {
            streamQuery.reset(new StreamQuery());
            streamQuery->query = query;
        }
    }
    return streamQuery;
}

bool LookupJoinKernel::genStreamKeys(const TablePtr &input, size_t offset, size_t count,
                                     vector<string>& pks) {
    string triggerField;
    if (!getPkTriggerField(triggerField)) {
        SQL_LOG(ERROR, "get pk trigger field failed");
        return false;
    }
    size_t rowCount = _inputTable->getRowCount();
    for (size_t rowOffset = offset; rowOffset < rowCount && rowOffset < offset + count; rowOffset++) {
        auto row = input->getRow(rowOffset);
        if (!genStreamQueryTerm(input, row, triggerField, pks)) {
            return false;
        }
    }
    return true;
}

bool LookupJoinKernel::getPkTriggerField(string& triggerField) {
    if (_leftIsBuild) {
        return getPkTriggerField(_rightJoinColumns, _leftJoinColumns, _leftIndexInfos, triggerField);
    } else {
        return getPkTriggerField(_leftJoinColumns, _rightJoinColumns, _rightIndexInfos, triggerField);
    }
}

bool LookupJoinKernel::getPkTriggerField(const vector<string>& inputFields,
                                         const vector<string>& joinFields,
                                         const map<string, sql::IndexInfo>& indexInfoMap,
                                         string& triggerField)
{
    for (const auto& kv : indexInfoMap) {
        const auto& indexInfo = kv.second;
        if ("primary_key" != indexInfo.type) {
            continue;
        }
        for (size_t i = 0; i < joinFields.size(); ++i) {
            if (indexInfo.name != joinFields[i]) {
                continue;
            }
            if (unlikely(i >= inputFields.size())) {
                return false;
            }
            triggerField = inputFields[i];
            return true;
        }
        return false;
    }
    return false;
}

QueryPtr LookupJoinKernel::genStreamQuery(const TablePtr &input, size_t offset, size_t count) {
    QueryPtr outQuery;
    OrQuery* orQuery = new OrQuery("");
    outQuery.reset(orQuery);
    size_t rowCount = input->getRowCount();
    for (size_t rowOffset = offset; rowOffset < rowCount && rowOffset < offset + count; rowOffset++) {
        QueryPtr streamQuery = genStreamQuery(input->getRow(rowOffset));
        if (streamQuery == nullptr) {
            SQL_LOG(DEBUG, "generate stream query failed, offset [%ld], table [%s]",
                    rowOffset, TableUtil::rowToString(input, rowOffset).c_str());
            continue;
        } else {
            orQuery->addQuery(streamQuery);
        }
    }
    const vector<QueryPtr>& queryVec = *(orQuery->getChildQuery());
    if (queryVec.size() == 0) {
        return {};
    } else if (queryVec.size() == 1) {
        return queryVec[0];
    } else {
        return outQuery;
    }
}

QueryPtr LookupJoinKernel::genStreamQuery(Row row) {
    if (_leftIsBuild) {
        return genStreamQuery(_inputTable, row, _rightJoinColumns, _leftJoinColumns, _leftIndexInfos);
    } else {
        return genStreamQuery(_inputTable, row, _leftJoinColumns, _rightJoinColumns, _rightIndexInfos);
    }
}

QueryPtr LookupJoinKernel::genStreamQuery(const TablePtr &input, Row row,
        const vector<string> &inputFields,
        const vector<string> &joinFields,
        const map<string, IndexInfo> &indexInfos)
{
    if (joinFields.empty() || inputFields.size() != joinFields.size()) {
        return {};
    }
    vector<QueryPtr> querys;
    for (size_t i = 0; i < inputFields.size(); i++) {
        auto iter = indexInfos.find(joinFields[i]);
        if (iter == indexInfos.end()) {
            continue;
        }
        QueryPtr query = genStreamQuery(input, row, inputFields[i], joinFields[i], iter->second.name, iter->second.type);
        if (query == nullptr) {
            SQL_LOG(DEBUG, "generate stream query failed,input field [%s] join field [%s]",
                    inputFields[i].c_str(), joinFields[i].c_str());
            return {};
        }
        querys.push_back(query);
    }
    if (querys.empty()) {
        return {};
    } else if (1 == querys.size()) {
        return querys[0];
    } else {
        QueryPtr andQuery(new AndQuery(""));
        for (const auto& query : querys) {
            andQuery->addQuery(query);
        }
        return andQuery;
    }
}

QueryPtr LookupJoinKernel::genStreamQuery(const TablePtr &input, Row row, const string &inputField,
                                          const string &joinField, const string& indexName,
                                          const string& indexType)
{
    vector<string> termVec;
    if (!genStreamQueryTerm(input, row, inputField, termVec)) {
        return {};
    }
    QueryPtr query;
    if (indexType == "number") {
        if (termVec.size() == 1) {
            int64_t numVal = StringUtil::numberFromString<int64_t>(termVec[0]);
            NumberTerm term(numVal, indexName.c_str(), RequiredFields());
            query.reset(new NumberQuery(term, ""));
        } else if (termVec.size() > 1) {
            MultiTermQuery *multiTermQuery = new MultiTermQuery("", OP_OR);
            for (auto &termStr : termVec) {
                int64_t numVal = StringUtil::numberFromString<int64_t>(termStr);
                TermPtr term(new NumberTerm(numVal, indexName.c_str(), RequiredFields()));
                multiTermQuery->addTerm(term);
            }
            query.reset(multiTermQuery);
        }
    } else {
        if (termVec.size() == 1) {
            query.reset(new TermQuery(termVec[0], indexName, RequiredFields(), ""));
        } else if (termVec.size() > 1) {
            MultiTermQuery *multiTermQuery = new MultiTermQuery("", OP_OR);
            for (auto &termStr : termVec) {
                TermPtr term(new Term(termStr, indexName.c_str(), RequiredFields()));
                multiTermQuery->addTerm(term);
            }
            query.reset(multiTermQuery);
        }
    }
    return query;
}

bool LookupJoinKernel::genStreamQueryTerm(const TablePtr &inputTable, Row row,
        const string &inputField, vector<string> &termVec)
{
    ColumnPtr joinColumn = inputTable->getColumn(inputField);
    if (joinColumn == nullptr) {
        SQL_LOG(WARN, "invalid join column name [%s]", inputField.c_str());
        return false;
    }
    auto schema = joinColumn->getColumnSchema();
    if (schema == nullptr) {
        SQL_LOG(WARN, "get left join column [%s] schema failed", inputField.c_str());
        return false;
    }
    auto vt = schema->getType();
    bool isMulti = vt.isMultiValue();
    switch (vt.getBuiltinType()) {
#define CASE_MACRO(ft)                                                  \
        case ft: {                                                      \
            if (isMulti) {                                              \
                typedef MatchDocBuiltinType2CppType<ft, true>::CppType T; \
                auto columnData = joinColumn->getColumnData<T>();       \
                if (unlikely(!columnData)) {                            \
                    SQL_LOG(ERROR, "impossible cast column data failed"); \
                    return false;                                       \
                }                                                       \
                const auto &datas = columnData->get(row);                 \
                for (size_t k = 0; k < datas.size(); k++) {             \
                    termVec.emplace_back(StringUtil::toString(datas[k])); \
                }                                                       \
            } else {                                                    \
                typedef MatchDocBuiltinType2CppType<ft, false>::CppType T; \
                auto columnData = joinColumn->getColumnData<T>();       \
                if (unlikely(!columnData)) {                            \
                    SQL_LOG(ERROR, "impossible cast column data failed"); \
                    return false;                                       \
                }                                                       \
                const auto &data = columnData->get(row);                 \
                termVec.emplace_back(StringUtil::toString(data));   \
            }                                                           \
            break;                                                      \
        }
        BUILTIN_TYPE_MACRO_HELPER(CASE_MACRO);
#undef CASE_MACRO
    default: {
        SQL_LOG(ERROR, "impossible reach this branch");
        return false;
    }
    }
    return true;
}

void LookupJoinKernel::patchLookupHintInfo(const map<string, string> &hintsMap) {
    if (hintsMap.empty()) {
        return;
    }
    auto iter = hintsMap.find("lookupBatchSize");
    if (iter != hintsMap.end()) {
        uint32_t lookupBatchSize = 0;
        StringUtil::fromString(iter->second, lookupBatchSize);
        if (lookupBatchSize > 0) {
            _lookupBatchSize = lookupBatchSize;
        }
    }
    iter = hintsMap.find("lookupTurncateThreshold");
    if (iter != hintsMap.end()) {
        uint32_t lookupTurncateSize = 0;
        StringUtil::fromString(iter->second, lookupTurncateSize);
        if (lookupTurncateSize > 0) {
            _lookupTurncateThreshold = lookupTurncateSize;
        }
    }
    iter = hintsMap.find("lookupDisableCacheFields");
    if (iter != hintsMap.end()) {
        auto fields = StringUtil::split(iter->second, ",");
        _disableCacheFields.insert(fields.begin(), fields.end());
    }
    iter = hintsMap.find("lookupEnableRowDeduplicate");
    if (iter != hintsMap.end()) {
        _enableRowDeduplicate = (iter->second == "yes");
    }
}
bool LookupJoinKernel::canTurncate() {
    if (_lookupTurncateThreshold > 0 && _hasJoinedCount >= _lookupTurncateThreshold) {
        return true;
    }
    return false;
}

ColumnTerm* LookupJoinKernel::makeColumnTerm(
        const TablePtr &input,
        const string &inputField,
        const string &joinField,
        size_t offset,
        size_t endOffset)
{
    unique_ptr<ColumnTerm> columnTerm;
    size_t rowCount = endOffset - offset;
    auto col = input->getColumn(inputField);
    if (! col) {
        SQL_LOG(WARN, "invalid join column name [%s]", inputField.c_str());
        return nullptr;
    }
    auto schema = col->getColumnSchema();
    if (! schema) {
        SQL_LOG(WARN, "get join column [%s] schema failed", inputField.c_str());
        return nullptr;
    }
    bool enableCache = (_disableCacheFields.count(joinField) == 0);
    auto f1 = [&](auto a) -> bool {
        using T = typename decltype(a)::value_type;
        auto p = new ColumnTermTyped<T>(joinField);
        columnTerm.reset(p);
        p->setEnableCache(enableCache);
        auto &values = p->getValues();
        values.reserve(rowCount);
        auto colData = col->getColumnData<T>();
        if (unlikely(! colData)) {
            SQL_LOG(ERROR, "impossible cast column data failed");
            columnTerm.reset();
            return false;
        }
        for (size_t i = offset; i < endOffset; ++i) {
            const auto &row = input->getRow(i);
            const auto &data = colData->get(row);
            values.push_back(data);
        }
        return true;
    };
    auto f2 = [&](auto a) {
        using Multi = typename decltype(a)::value_type;
        using Single = typename Multi::value_type;
        auto p = new ColumnTermTyped<Single>(joinField);
        columnTerm.reset(p);
        p->setEnableCache(enableCache);
        auto &values = p->getValues();
        auto &offsets = p->getOffsets();
        values.reserve(rowCount * 4);
        offsets.reserve(rowCount + 1);
        offsets.push_back(0);
        auto colData = col->getColumnData<Multi>();
        if (unlikely(! colData)) {
            SQL_LOG(ERROR, "impossible cast column data failed");
            columnTerm.reset();
            return false;
        }
        for (size_t i = offset; i < endOffset; ++i) {
            const auto &row = input->getRow(i);
            const auto &datas = colData->get(row);
            auto dataSize = datas.size();
            for (size_t j = 0; j < dataSize; ++j) {
                values.push_back(datas[j]);
            }
            offsets.push_back(values.size());
        }
        return true;
    };
    if (! ValueTypeSwitch::switchType(schema->getType(), f1, f2)) {
        SQL_LOG(ERROR, "gen multi term optimize query failed");
        return nullptr;
    }
    return columnTerm.release();
}

QueryPtr LookupJoinKernel::genTableQuery(
        const TablePtr &input, size_t offset, size_t count,
        const vector<string> &inputFields,
        const vector<string> &joinFields,
        const map<string, sql::IndexInfo> &indexInfos)
{
    if (_enableRowDeduplicate) {
        return genTableQueryWithRowOptimized(input, offset, count, inputFields, joinFields, indexInfos);
    }
    TableQueryPtr tableQuery(new TableQuery(string()));
    auto &terms = tableQuery->getTermArray();
    terms.reserve(inputFields.size());
    size_t rowCount = input->getRowCount();
    size_t endOffset = min(rowCount, offset + count);
    for (size_t i = 0; i < inputFields.size(); i++) {
        auto iter = indexInfos.find(joinFields[i]);
        if (iter == indexInfos.end()) {
            continue;
        }
        ColumnTerm* term = makeColumnTerm(input, inputFields[i], joinFields[i], offset, endOffset);
        if (term) {
            terms.push_back(term);
        }
    }
    return tableQuery;
}

QueryPtr LookupJoinKernel::genTableQuery(const TablePtr &input, size_t offset, size_t count) {
    if (_leftIsBuild) {
        return genTableQuery(input, offset, count, _rightJoinColumns, _leftJoinColumns, _leftIndexInfos);
    } else {
        return genTableQuery(input, offset, count, _leftJoinColumns, _rightJoinColumns, _rightIndexInfos);
    }
}

bool LookupJoinKernel::genrowGroupKey(
        const TablePtr &input,
        const vector<ColumnPtr> &singleTermCols,
        size_t offset,
        size_t endOffset,
        vector<string> &rowGroupKey)
{
    size_t idx;
    auto f0 = [&](auto a) {
        return false;
    };
    auto f1 = [&](auto a) -> bool {
        using T = typename decltype(a)::value_type;
        auto colData = singleTermCols[idx]->getColumnData<T>();
        if (unlikely(! colData)) {
            SQL_LOG(ERROR, "impossible cast column data failed");
            return false;
        }
        for (size_t i = offset; i < endOffset; ++i) {
            const auto &row = input->getRow(i);
            const auto &data = colData->get(row);
            rowGroupKey[i - offset] += "#$" + StringUtil::toString(data);
        }
        return true;
    };
    for (idx = 0; idx < singleTermCols.size(); ++idx) {
        if (! ValueTypeSwitch::switchType(singleTermCols[idx]->getColumnSchema()->getType(), f1, f0)) {
            SQL_LOG(ERROR, "gen single term hash value failed");
            return false;
        }
    }
    return true;
}

bool LookupJoinKernel::genSingleTermColumns(
        const TablePtr &input,
        const vector<ColumnPtr> &singleTermCols,
        const vector<size_t> &singleTermID,
        const vector<string> &joinFields,
        const unordered_map<string, vector<size_t>> &rowGroups,
        size_t rowCount,
        vector<ColumnTerm*> &terms)
{
    size_t idx;
    auto f0 = [&](auto a) {
        return false;
    };
    auto f1 = [&](auto a) -> bool {
        using T = typename decltype(a)::value_type;
        auto p = new ColumnTermTyped<T>(joinFields[singleTermID[idx]]);
        bool enableCache = (_disableCacheFields.count(joinFields[singleTermID[idx]]) == 0);
        p->setEnableCache(enableCache);
        auto &values = p->getValues();
        values.reserve(rowCount);
        auto colData = singleTermCols[idx]->getColumnData<T>();
        if (unlikely(! colData)) {
            SQL_LOG(ERROR, "impossible cast column data failed");
            delete p;
            return false;
        }
        for (const auto& m : rowGroups) {
            const auto &row = input->getRow(m.second[0]);
            const auto &data = colData->get(row);
            values.push_back(data);
        }
        terms.push_back(p);
        return true;
    };
    for (idx = 0; idx < singleTermCols.size(); ++idx) {
        if (! ValueTypeSwitch::switchType(singleTermCols[idx]->getColumnSchema()->getType(), f1, f0)) {
            SQL_LOG(ERROR, "gen single term hash value failed");
            return false;
        }
    }
    return true;
}

bool LookupJoinKernel::genMultiTermColumns(
        const TablePtr &input,
        const vector<ColumnPtr> &multiTermCols,
        const vector<size_t> &multiTermID,
        const vector<string> &joinFields,
        const unordered_map<string, vector<size_t>> &rowGroups,
        size_t rowCount,
        vector<ColumnTerm*> &terms)
{
    auto f0 = [&](auto a) {
        return false;
    };
    auto f1 = [&](auto a) {
        using Multi = typename decltype(a)::value_type;
        using Single = typename Multi::value_type;
        auto p = new ColumnTermTyped<Single>(joinFields[multiTermID[0]]);
        bool enableCache = (_disableCacheFields.count(joinFields[multiTermID[0]]) == 0);
        p->setEnableCache(enableCache);
        auto &values = p->getValues();
        auto &offsets = p->getOffsets();
        values.reserve(rowCount * 4);
        offsets.reserve(rowGroups.size() + 1);
        offsets.push_back(0);
        auto colData = multiTermCols[0]->getColumnData<Multi>();
        if (unlikely(! colData)) {
            SQL_LOG(ERROR, "impossible cast column data failed");
            delete p;
            return false;
        }
        for (const auto& m : rowGroups) {
            unordered_set<Single> uni;
            for (auto i : m.second) {
                const auto &row = input->getRow(i);
                const auto &datas = colData->get(row);
                for (size_t j = 0; j < datas.size(); ++j) {
                    uni.insert(datas[j]);
                }
            }
            values.insert(values.end(), uni.begin(), uni.end());
            offsets.push_back(values.size());
        }
        terms.push_back(p);
        return true;
    };
    if (! multiTermCols.empty()) {
        if (! ValueTypeSwitch::switchType(multiTermCols[0]->getColumnSchema()->getType(), f0, f1)) {
            SQL_LOG(ERROR, "gen multi term hash value failed");
            return false;
        }
    }
    return true;
}

QueryPtr LookupJoinKernel::genTableQueryWithRowOptimized(
        const TablePtr &input, size_t offset, size_t count,
        const vector<string> &inputFields,
        const vector<string> &joinFields,
        const map<string, sql::IndexInfo> &indexInfos)
{
    TableQueryPtr tableQuery(new TableQuery(string()));
    auto &terms = tableQuery->getTermArray();
    size_t inputFieldsCount = inputFields.size();
    terms.reserve(inputFieldsCount);
    size_t rowCount = input->getRowCount();
    size_t endOffset = min(rowCount, offset + count);
    vector<ColumnPtr> singleTermCols, multiTermCols;
    vector<size_t> singleTermID, multiTermID;
    for (size_t i = 0; i < inputFieldsCount; i++) {
        auto iter = indexInfos.find(joinFields[i]);
        //是否存在找不到的情况
        if (iter == indexInfos.end()) {
            continue;
        }
        auto p = input->getColumn(inputFields[i]);
        if (p->getColumnSchema()->getType().isMultiValue()) {
            multiTermCols.push_back(p);
            multiTermID.push_back(i);
        }else {
            singleTermCols.push_back(p);
            singleTermID.push_back(i);
        }
    }
    vector<string> rowGroupKey(endOffset - offset);
    if(! genrowGroupKey(input, singleTermCols, offset, endOffset, rowGroupKey)) {
        return {};
    }
    unordered_map<string, vector<size_t>> rowGroups;
    for (size_t i = 0; i < rowGroupKey.size(); ++i) {
        rowGroups[rowGroupKey[i]].push_back(i + offset);
    }
    if(! genSingleTermColumns(input, singleTermCols, singleTermID, joinFields, rowGroups, rowCount, terms)) {
        return {};
    }
    if(! genMultiTermColumns(input, multiTermCols, multiTermID, joinFields, rowGroups, rowCount, terms)) {
        return {};
    }
    return tableQuery;
}
REGISTER_KERNEL(LookupJoinKernel);

END_HA3_NAMESPACE(sql);
