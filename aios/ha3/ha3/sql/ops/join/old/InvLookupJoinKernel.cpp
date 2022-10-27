#include <ha3/sql/ops/join/InvLookupJoinKernel.h>
#include <ha3/sql/ops/util/KernelRegister.h>
#include <ha3/sql/ops/scan/ScanIterator.h>
#include <ha3/search/IndexPartitionReaderUtil.h>
#include <ha3/turing/ops/SqlSessionResource.h>

using namespace std;
using namespace matchdoc;
using namespace suez::turing;
USE_HA3_NAMESPACE(search);

BEGIN_HA3_NAMESPACE(sql);

InvLookupJoinKernel::InvLookupJoinKernel()
    : _useSub(false)
{
}

InvLookupJoinKernel::~InvLookupJoinKernel() {
    _attributeExpressionVec.clear();
}

const navi::KernelDef *InvLookupJoinKernel::getDef() const {
    auto def = new navi::KernelDef();
    def->set_kernel_name("InvLookupJoinKernel");
    KERNEL_REGISTER_ADD_INPUT(def, "input0");
    KERNEL_REGISTER_ADD_OUTPUT(def, "output0");
    return def;
}

bool InvLookupJoinKernel::config(navi::KernelConfigContext &ctx) {
    auto &json = ctx.getJsonAttrs();
    json.Jsonize("inv_table_name", _invTableName);
    json.Jsonize("inv_table_indexes", _invTableIndexes);
    json.Jsonize("inv_table_use_sub", _useSub, _useSub);
    KernelUtil::stripName(_invTableIndexes);
    return JoinKernelBase::config(ctx);
}

navi::ErrorCode InvLookupJoinKernel::doInit(SqlBizResource *bizResource,
        SqlQueryResource *queryResource)
{
    _indexPartitionReaderWrapper = IndexPartitionReaderUtil::createIndexPartitionReaderWrapper(
            queryResource->getPartitionReaderSnapshot(), _invTableName);
    KERNEL_REQUIRES(_indexPartitionReaderWrapper, "create index partition reader wrapper failed.");
    _indexPartitionReaderWrapper->setSessionPool(_pool);
    TableInfoPtr tableInfo = bizResource->getTableInfo(_invTableName);
    if (!tableInfo) {
        SQL_LOG(ERROR, "table [%s] not exist.", _invTableName.c_str());
        return navi::EC_ABORT;
    }
    if (!initExpressionCreator(tableInfo, _useSub,  bizResource, queryResource)) {
        SQL_LOG(ERROR, "table [%s] init expression creator failed.",
                _invTableName.c_str());
        return navi::EC_ABORT;
    }
    if (!initScanColumn()) {
        SQL_LOG(ERROR, "table [%s] init scan column failed.", _invTableName.c_str());
        return navi::EC_ABORT;
    }
    _rightTable.reset(new Table({}, _matchDocAllocator));

    auto sqlSessionResource =
        bizResource->getObject<turing::SqlSessionResource>("SqlSessionResource");
    _param.tableName = _invTableName;
    _param.indexPartitionReaderWrapper = _indexPartitionReaderWrapper;
    _param.attributeExpressionCreator = _attributeExpressionCreator;
    _param.matchDocAllocator = _matchDocAllocator;
    _param.tableInfo = tableInfo;
    _param.pool = _pool;
    _param.timeoutTerminator = queryResource->getTimeoutTerminator();
    if (sqlSessionResource != nullptr) {
        _param.analyzerFactory = sqlSessionResource->analyzerFactory.get();
        _param.queryInfo = &sqlSessionResource->queryInfo;
    } else {
        SQL_LOG(INFO, "table [%s] get sql session resource failed.",
                _invTableName.c_str());
    }
    return navi::EC_NONE;
}

bool InvLookupJoinKernel::initExpressionCreator(const TableInfoPtr &tableInfo,
                                                bool useSub,
                                                SqlBizResource *bizResource,
                                                SqlQueryResource *queryResource)
{
    bool useSubFlag = useSub && tableInfo->hasSubSchema();
    _matchDocAllocator.reset(
        new MatchDocAllocator(_memoryPoolResource->getPool(), useSubFlag));
    _functionProvider.reset(new FunctionProvider(_matchDocAllocator.get(),
                    _pool, queryResource->getCavaAllocator(), queryResource->getTracer().get(),
                    queryResource->getPartitionReaderSnapshot(), NULL));
    std::vector<suez::turing::VirtualAttribute*> virtualAttributes;
    _attributeExpressionCreator.reset(new AttributeExpressionCreator(
                    _pool, _matchDocAllocator.get(),
                    _invTableName, queryResource->getPartitionReaderSnapshot(),
                    tableInfo, virtualAttributes,
                    bizResource->getFunctionInterfaceCreator().get(),
                    bizResource->getCavaPluginManager(), _functionProvider.get()));
     return true;
}

bool InvLookupJoinKernel::initScanColumn() {
    _attributeExpressionVec.clear();
    _attributeExpressionVec.reserve(_rightInputFields.size());
    for (auto &columnName : _rightInputFields) {
        auto *expr = _attributeExpressionCreator->createAttributeExpression(columnName);
        if (!expr || !expr->allocate(_matchDocAllocator.get())) {
            SQL_LOG(ERROR, "Create attribute expression failed! columnName[%s]",
                            columnName.c_str());
            return false;
        }
        auto refer = expr->getReferenceBase();
        refer->setSerializeLevel(SL_ATTRIBUTE);
        matchdoc::ValueType valueType = refer->getValueType();
        valueType._ha3ReservedFlag = 1;
        refer->setValueType(valueType);
        _attributeExpressionVec.push_back(expr);
    }
    _matchDocAllocator->extend();
    _evaluator.reset(new ExpressionEvaluator<vector<AttributeExpression *> >(
            _attributeExpressionVec,
            _matchDocAllocator->getSubDocAccessor()));
    return true;
}

bool InvLookupJoinKernel::singleJoin(ColumnPtr joinColumn, ValueType vt, size_t rowCount) {
    TablePtr output(new Table(_memoryPoolResource->getPool()));
    switch (vt.getBuiltinType()) {
#define CASE_MACRO(ft)                                                  \
        case ft: {                                                      \
            typedef MatchDocBuiltinType2CppType<ft, false>::CppType T;  \
            auto columnData = joinColumn->getColumnData<T>();           \
            if (unlikely(!columnData)) {                                \
                SQL_LOG(ERROR, "impossible cast column data failed"); \
                return false;                                           \
            }                                                           \
            for (size_t i = 0; i < rowCount; i++) {                     \
                const string &joinValue = columnData->toString(i);          \
                if (!seekAndJoin(joinValue, i)) {                           \
                    SQL_LOG(ERROR, "seek and join failed");     \
                    return false;                                       \
                }                                                       \
            }                                                           \
            break;                                                      \
        }
        BUILTIN_TYPE_MACRO_HELPER(CASE_MACRO);
#undef CASE_MACRO
        default: {
            SQL_LOG(ERROR, "impossible reach this branch");
            return false;
        }
    } //switch

    return true;
}

bool InvLookupJoinKernel::multiJoin(ColumnPtr joinColumn, ValueType vt, size_t rowCount) {
    TablePtr output(new Table(_memoryPoolResource->getPool()));
    switch (vt.getBuiltinType()) {
#define CASE_MACRO(ft)                                                  \
        case ft: {                                                      \
            typedef MatchDocBuiltinType2CppType<ft, true>::CppType T;  \
            auto columnData = joinColumn->getColumnData<T>();           \
            if (unlikely(!columnData)) {                                \
                SQL_LOG(ERROR, "impossible cast column data failed"); \
                return false;                                           \
            }                                                           \
            for (size_t i = 0; i < rowCount; i++) {                     \
                T joinValues = columnData->get(i);                      \
                for (size_t k = 0; k < joinValues.size(); ++k) {        \
                    if (!seekAndJoin(autil::StringUtil::toString(joinValues[k]), i)) { \
                        SQL_LOG(ERROR, "seek and join failed"); \
                        return false;                                   \
                    }                                                   \
                }                                                       \
            }                                                           \
            break;                                                      \
        }
        BUILTIN_TYPE_MACRO_HELPER(CASE_MACRO);
#undef CASE_MACRO
        default: {
            SQL_LOG(ERROR, "impossible reach this branch");
            return false;
        }
    } //switch

    return true;
}

bool InvLookupJoinKernel::seekAndJoin(const string &joinValue, size_t leftIndex)
{
    bool emptyScan = false;
    auto scanIter = createScanIter(joinValue, emptyScan);
    if (scanIter == nullptr && !emptyScan) {
        SQL_LOG(ERROR, "create ha3 scan iterator for right table [%s] failed",
                _invTableName.c_str());
        return false;
    }
    if (emptyScan) {
        return true;
    }

    vector<MatchDoc> matchDocs;
    scanIter->batchSeek(0, matchDocs);
    _evaluator->batchEvaluateExpressions(&matchDocs[0], matchDocs.size());
    size_t offset = _rightTable->getRowCount();
    for (size_t i = 0; i < matchDocs.size(); i++) {
        joinRow(leftIndex, offset + i);
    }
    _rightTable->appendRows(matchDocs);
    DELETE_AND_SET_NULL(scanIter);
    SQL_LOG(DEBUG, "tmp joined table for join value[%s:%s] col[%zu] row[%zu]",
            _joinField.begin()->first.c_str(), joinValue.c_str(),
            _rightTable->getColumnCount(), _rightTable->getRowCount());
    return true;
}

ScanIterator *InvLookupJoinKernel::createScanIter(const string &joinValue, bool &emptyScan) {
    // todo: join condition
    const auto &rightJoinField = _joinField.begin()->second;
    string conditionJson = "{\"op\":\"=\", \"params\":[\"$" + rightJoinField +
                           "\", \"" + joinValue + "\"]}";
    ScanIteratorCreator scanIterCreator(_param);
    return scanIterCreator.createScanIterator(conditionJson, _invTableIndexes, emptyScan);
}

TablePtr InvLookupJoinKernel::createTable(const vector<Row> &matchDocs,
                                          const MatchDocAllocatorPtr &matchDocAllocator)
{
    bool hasSub = false;
    const ReferenceVector &refs = matchDocAllocator->getRefBySerializeLevel(SL_ATTRIBUTE);
    for (ReferenceBase *ref : refs) {
        if (ref->isSubDocReference()) {
            hasSub = true;
            break;
        }
    }

    TablePtr table;
    if (hasSub) {
        vector<MatchDoc> newMatchDocs;
        auto accessor = matchDocAllocator->getSubDocAccessor();
        std::function<void(MatchDoc)> processNothing =
            [&newMatchDocs](MatchDoc newDoc) { newMatchDocs.push_back(newDoc); };
        for (MatchDoc doc : matchDocs) {
            accessor->foreachFlatten(doc, matchDocAllocator.get(), processNothing);
        }
        table.reset(new Table(newMatchDocs, matchDocAllocator));
        return table;
    } else {
        table.reset(new Table(matchDocs, matchDocAllocator));
        return table;
    }
}

REGISTER_KERNEL(InvLookupJoinKernel);

END_HA3_NAMESPACE(sql);
