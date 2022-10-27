#include <ha3/sql/ops/join/KvLookupJoinKernel.h>
#include <ha3/sql/ops/util/KernelRegister.h>
#include <indexlib/indexlib.h>
#include "indexlib/common/field_format/pack_attribute/pack_attribute_formatter.h"
using namespace std;
using namespace matchdoc;
using namespace suez::turing;
using namespace heavenask::indexlib;
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(config);

BEGIN_HA3_NAMESPACE(sql);

KvLookupJoinKernel::KvLookupJoinKernel()
    : _valueCollectorPtr(new ValueCollector())
{
}

KvLookupJoinKernel::~KvLookupJoinKernel() {
}

const navi::KernelDef *KvLookupJoinKernel::getDef() const {
    auto def = new navi::KernelDef();
    def->set_kernel_name("KvLookupJoinKernel");
    KERNEL_REGISTER_ADD_INPUT(def, "input0");
    KERNEL_REGISTER_ADD_OUTPUT(def, "output0");
    return def;
}

bool KvLookupJoinKernel::config(navi::KernelConfigContext &ctx) {
    ctx.getJsonAttrs().Jsonize("kv_table_name", _kvTableName);
    return JoinKernelBase::config(ctx);
}

navi::ErrorCode KvLookupJoinKernel::doInit(SqlBizResource *bizResource,
        SqlQueryResource *queryResource)
{
    auto partReader = queryResource->getPartitionReaderSnapshot()->
                      GetIndexPartitionReader(_kvTableName);
    if (!partReader) {
        SQL_LOG(ERROR, "get partition reader failed from table [%s]", _kvTableName.c_str());
        return navi::EC_ABORT;
    }
    auto partSchema = partReader->GetSchema();
    if (!partSchema) {
        SQL_LOG(ERROR, "get schema failed from table [%s]", _kvTableName.c_str());
        return navi::EC_ABORT;
    }
    if (partSchema->GetTableType() != tt_kv) {
        SQL_LOG(ERROR, "table [%s] is not kv table", _kvTableName.c_str());
        return navi::EC_ABORT;
    }
    _kvReader = partReader->GetKVReader();
    if (!_kvReader) {
        SQL_LOG(ERROR, "table [%s] get kv reader failed", _kvTableName.c_str());
        return navi::EC_ABORT;
    }
    MatchDocAllocatorPtr rightTableAllocator =
        createMatchDocAllocator(partSchema, _memoryPoolResource->getPool());
    if (!rightTableAllocator) {
        SQL_LOG(ERROR, "table [%s] create matchdoc allocator failed", _kvTableName.c_str());
        return navi::EC_ABORT;
    }
    if (!_valueCollectorPtr->init(partSchema, rightTableAllocator)) {
        SQL_LOG(ERROR, "table [%s] create valueCollector failed", _kvTableName.c_str());
        return navi::EC_ABORT;
    }
    _rightTable.reset(new Table({}, rightTableAllocator));
    return navi::EC_NONE;
}

MatchDocAllocatorPtr KvLookupJoinKernel::createMatchDocAllocator(
        const IndexPartitionSchemaPtr &schema,
        const std::shared_ptr<autil::mem_pool::Pool> &poolPtr)
{
    string tableName = schema->GetSchemaName();
    MountInfoPtr mountInfo(new MountInfo());
    uint32_t beginMountId = 0;
    InsertPackAttributeMountInfo(mountInfo, schema, tableName, beginMountId);
    MatchDocAllocatorPtr allocator(new MatchDocAllocator(poolPtr, false, mountInfo));
    return allocator;
}

void KvLookupJoinKernel::InsertPackAttributeMountInfo(
        const MountInfoPtr& singleMountInfo,
        const IndexPartitionSchemaPtr& schema,
        const string &tableName, uint32_t& beginMountId)
{
    SingleFieldIndexConfigPtr pkIndexConfig =
        schema->GetIndexSchema()->GetPrimaryKeyIndexConfig();
    assert(pkIndexConfig);

    KVIndexConfigPtr kvIndexConfig = DYNAMIC_POINTER_CAST(KVIndexConfig, pkIndexConfig);
    assert(kvIndexConfig);
    const ValueConfigPtr& valueConfig = kvIndexConfig->GetValueConfig();
    assert(valueConfig);
    PackAttributeConfigPtr packConf = valueConfig->CreatePackAttributeConfig();
    if (valueConfig->GetAttributeCount() == 1
        && !valueConfig->GetAttributeConfig(0)->IsMultiValue()
        && (valueConfig->GetAttributeConfig(0)->GetFieldType() != ft_string))
    {
        packConf.reset();
    }
    InsertPackAttributeMountInfo(singleMountInfo, packConf, tableName, beginMountId++);
}

void KvLookupJoinKernel::InsertPackAttributeMountInfo(
        const MountInfoPtr& singleMountInfo,
        const PackAttributeConfigPtr& packAttrConf,
        const string &tableName, uint32_t mountId)
{
    if (!packAttrConf) {
        return;
    }
    vector<string> subAttrNames;
    packAttrConf->GetSubAttributeNames(subAttrNames);

    PackAttributeFormatter packAttrFormatter;
    if (!packAttrFormatter.Init(packAttrConf)) {
        SQL_LOG(ERROR, "table [%s] init pack attribute formatter failed!", tableName.c_str());
        return ;
    }

    for (size_t i = 0; i < subAttrNames.size(); i++) {
        AttributeReference* attrRef =
                packAttrFormatter.GetAttributeReference(subAttrNames[i]);
        assert(attrRef);

        string fullName = tableName + ":" + subAttrNames[i];
        singleMountInfo->insert(fullName, mountId, attrRef->GetOffset());
        singleMountInfo->insert(subAttrNames[i], mountId, attrRef->GetOffset());
    }
}

bool KvLookupJoinKernel::singleJoin(ColumnPtr joinColumn, ValueType vt, size_t rowCount) {
    ConstString rightValue;
    switch (vt.getBuiltinType()) {
#define INT_CASE_CLAUSE(ft)                                             \
        case ft: {                                                      \
            typedef MatchDocBuiltinType2CppType<ft, false>::CppType T;  \
            auto columnData = joinColumn->getColumnData<T>();           \
            if (unlikely(!columnData)) {                                \
                SQL_LOG(ERROR, "impossible cast reference failed");   \
                return false;                                           \
            }                                                           \
            for (size_t i = 0; i < rowCount; ++i) {                     \
                T joinValue = columnData->get(i);                         \
                if (!_kvReader->Get((keytype_t)joinValue, rightValue, 0,  \
                                tsc_default, _pool))                    \
                {                                                       \
                    continue;                                           \
                }                                                       \
                auto rightRow = _rightTable->allocateRow();             \
                _valueCollectorPtr->collectValueFields(rightRow, rightValue); \
                _tableAIndexes.push_back(i);                        \
                _tableBIndexes.push_back(_rightTable->getRowCount() - 1); \
            }                                                           \
            break;                                                      \
        }
        INT_CASE_CLAUSE(matchdoc::bt_int8);
        INT_CASE_CLAUSE(matchdoc::bt_uint8);
        INT_CASE_CLAUSE(matchdoc::bt_int16);
        INT_CASE_CLAUSE(matchdoc::bt_uint16);
        INT_CASE_CLAUSE(matchdoc::bt_int32);
        INT_CASE_CLAUSE(matchdoc::bt_uint32);
        INT_CASE_CLAUSE(matchdoc::bt_int64);
        INT_CASE_CLAUSE(matchdoc::bt_uint64);
#undef INT_CASE_CLAUSE
    case bt_string: {
        auto columnData = joinColumn->getColumnData<MultiChar>();
        if (unlikely(!columnData)) {
            SQL_LOG(ERROR, "impossible cast reference failed");
            return false;
        }
        for (size_t i = 0; i < rowCount; ++i) {
            MultiChar joinValue = columnData->get(i);
            if (!_kvReader->Get(autil::ConstString(joinValue.data(), joinValue.size(), _pool),
                            rightValue, 0, tsc_default, _pool))
            {
                continue;
            }
            auto rightRow = _rightTable->allocateRow();
            _valueCollectorPtr->collectValueFields(rightRow, rightValue);
            _tableAIndexes.push_back(i);
            _tableBIndexes.push_back(_rightTable->getRowCount() - 1);
        }
        break;
    }
    default: {
        // impossible
        SQL_LOG(ERROR, "impossible reach this branch");
        return false;
    }
    }//switch
    return true;
}

bool KvLookupJoinKernel::multiJoin(ColumnPtr joinColumn, ValueType vt, size_t rowCount) {
    ConstString rightValue;
    switch (vt.getBuiltinType()) {
#define INT_CASE_CLAUSE(ft)                                             \
        case ft: {                                                      \
            typedef MatchDocBuiltinType2CppType<ft, true>::CppType T;  \
            auto columnData = joinColumn->getColumnData<T>();           \
            if (unlikely(!columnData)) {                                \
                SQL_LOG(ERROR, "impossible cast reference failed");   \
                return false;                                           \
            }                                                           \
            for (size_t i = 0; i < rowCount; ++i) {                     \
                T joinValues = columnData->get(i);                      \
                for (size_t k = 0; k < joinValues.size(); ++k) {        \
                    if (!_kvReader->Get((keytype_t)joinValues[k], rightValue, 0, \
                                    tsc_default, _pool))                \
                    {                                                   \
                        continue;                                       \
                    }                                                   \
                    auto rightRow = _rightTable->allocateRow();         \
                    _valueCollectorPtr->collectValueFields(rightRow, rightValue); \
                    _tableAIndexes.push_back(i);                        \
                    _tableBIndexes.push_back(_rightTable->getRowCount() - 1); \
                }                                                       \
            }                                                           \
            break;                                                      \
        }
        INT_CASE_CLAUSE(matchdoc::bt_int8);
        INT_CASE_CLAUSE(matchdoc::bt_uint8);
        INT_CASE_CLAUSE(matchdoc::bt_int16);
        INT_CASE_CLAUSE(matchdoc::bt_uint16);
        INT_CASE_CLAUSE(matchdoc::bt_int32);
        INT_CASE_CLAUSE(matchdoc::bt_uint32);
        INT_CASE_CLAUSE(matchdoc::bt_int64);
        INT_CASE_CLAUSE(matchdoc::bt_uint64);
#undef INT_CASE_CLAUSE
    case bt_string: {
        auto columnData = joinColumn->getColumnData<MultiString>();
        if (unlikely(!columnData)) {
            SQL_LOG(ERROR, "impossible cast reference failed");
            return false;
        }
        for (size_t i = 0; i < rowCount; ++i) {
            MultiString joinValues = columnData->get(i);
            for (size_t k = 0; k < joinValues.size(); ++k) {
                ConstString key(joinValues[k].data(), joinValues[k].size(), _pool);
                if (!_kvReader->Get(key, rightValue, 0, tsc_default, _pool)) {
                    continue;
                }
                auto rightRow = _rightTable->allocateRow();
                _valueCollectorPtr->collectValueFields(rightRow, rightValue);
                _tableAIndexes.push_back(i);
                _tableBIndexes.push_back(_rightTable->getRowCount() - 1);
            }
        }
        break;
    }
    default: {
        // impossible
        SQL_LOG(ERROR, "impossible reach this branch");
        return false;
    }
    }//switch
    return true;
}

REGISTER_KERNEL(KvLookupJoinKernel);

END_HA3_NAMESPACE(sql);
