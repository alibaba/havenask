#include <ha3/sql/ops/join/KkvLookupJoinKernel.h>
#include <ha3/sql/ops/util/KernelRegister.h>
#include <indexlib/indexlib.h>

using namespace std;
using namespace matchdoc;
using namespace suez::turing;
using namespace heavenask::indexlib;
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(config);

BEGIN_HA3_NAMESPACE(sql);

KkvLookupJoinKernel::KkvLookupJoinKernel()
    : _valueCollectorPtr(new ValueCollector())
{
}

KkvLookupJoinKernel::~KkvLookupJoinKernel() {
}

const navi::KernelDef *KkvLookupJoinKernel::getDef() const {
    auto def = new navi::KernelDef();
    def->set_kernel_name("KkvLookupJoinKernel");
    KERNEL_REGISTER_ADD_INPUT(def, "input0");
    KERNEL_REGISTER_ADD_OUTPUT(def, "output0");
    return def;
}

bool KkvLookupJoinKernel::config(navi::KernelConfigContext &ctx) {
    ctx.getJsonAttrs().Jsonize("kkv_table_name", _kkvTableName);
    return JoinKernelBase::config(ctx);
}

navi::ErrorCode KkvLookupJoinKernel::doInit(SqlBizResource *bizResource,
        SqlQueryResource *queryResource)
{
    auto partReader = queryResource->getPartitionReaderSnapshot()->
                      GetIndexPartitionReader(_kkvTableName);
    if (!partReader) {
        SQL_LOG(ERROR, "get partition reader failed from table [%s]",
                        _kkvTableName.c_str());
        return navi::EC_ABORT;
    }
    auto partSchema = partReader->GetSchema();
    if (!partSchema) {
        SQL_LOG(ERROR, "get schema failed from table [%s]", _kkvTableName.c_str());
        return navi::EC_ABORT;
    }
    if (partSchema->GetTableType() != tt_kkv) {
        SQL_LOG(ERROR, "table [%s] is not kkv table", _kkvTableName.c_str());
        return navi::EC_ABORT;
    }
    _kkvReader = partReader->GetKKVReader();
    if (!_kkvReader) {
        SQL_LOG(ERROR, "table [%s] get kkv reader failed", _kkvTableName.c_str());
        return navi::EC_ABORT;
    }
    MatchDocAllocatorPtr rightTableAllocator =
        createMatchDocAllocator(partSchema, _memoryPoolResource->getPool());
    if (!rightTableAllocator) {
        SQL_LOG(ERROR, "table [%s] create matchdoc allocator failed",
                        _kkvTableName.c_str());
        return navi::EC_ABORT;
    }
    if (!_valueCollectorPtr->init(partSchema, rightTableAllocator)) {
        SQL_LOG(ERROR, "table [%s] create valueCollector failed",
                        _kkvTableName.c_str());
        return navi::EC_ABORT;
    }
    _rightTable.reset(new Table({}, rightTableAllocator));
    return navi::EC_NONE;
}

MatchDocAllocatorPtr KkvLookupJoinKernel::createMatchDocAllocator(
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

void KkvLookupJoinKernel::InsertPackAttributeMountInfo(
        const MountInfoPtr& singleMountInfo,
        const IndexPartitionSchemaPtr& schema,
        const string &tableName, uint32_t& beginMountId)
{
    SingleFieldIndexConfigPtr pkIndexConfig =
        schema->GetIndexSchema()->GetPrimaryKeyIndexConfig();
    assert(pkIndexConfig);

    KKVIndexConfigPtr kkvIndexConfig = DYNAMIC_POINTER_CAST(
            KKVIndexConfig, pkIndexConfig);
    assert(kkvIndexConfig);
    const ValueConfigPtr& valueConfig = kkvIndexConfig->GetValueConfig();
    assert(valueConfig);
    PackAttributeConfigPtr packConf = valueConfig->CreatePackAttributeConfig();
    InsertPackAttributeMountInfo(singleMountInfo, packConf, tableName, beginMountId++);
}

void KkvLookupJoinKernel::InsertPackAttributeMountInfo(
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
        SQL_LOG(ERROR, "table [%s] init pack attribute formatter failed!",
                        tableName.c_str());
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

bool KkvLookupJoinKernel::singleJoin(ColumnPtr joinColumn, ValueType vt, size_t rowCount) {
    int64_t timestamp = TimeUtility::currentTime();
    auto deleter = [this](KKVIterator *p) { IE_POOL_COMPATIBLE_DELETE_CLASS(_pool, p); };
    unique_ptr<KKVIterator, decltype(deleter)> kkvIter(nullptr, deleter);

    for (size_t i = 0; i < rowCount; ++i) {
        switch (vt.getBuiltinType()) {
#define INT_CASE_CLAUSE(ft)                                             \
            case ft: {                                                  \
                typedef MatchDocBuiltinType2CppType<ft, false>::CppType T; \
                auto columnData = joinColumn->getColumnData<T>();       \
                if (unlikely(!columnData)) {                            \
                    SQL_LOG(ERROR, "impossible cast reference failed"); \
                    return false;                                       \
                }                                                       \
                T joinValue = columnData->get(i);                         \
                kkvIter.reset(_kkvReader->Lookup((PKeyType)joinValue, timestamp, tsc_default, _pool)); \
                break;                                                  \
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
            MultiChar joinValue = columnData->get(i);
            ConstString key(joinValue.data(), joinValue.size(), _pool);
            kkvIter.reset(_kkvReader->Lookup(key, timestamp, tsc_default, _pool));
            break;
        }
        default: {
            // impossible
            SQL_LOG(ERROR, "impossible reach this branch");
            return false;
        }
        } // switch

        if (kkvIter == nullptr) {
            SQL_LOG(WARN, "get kkv iter failed");
            continue;
        }
        while (kkvIter->IsValid()) {
            auto rightRow = _rightTable->allocateRow();
            _valueCollectorPtr->collectValueFields(rightRow, kkvIter.get());
            joinRow(i, _rightTable->getRowCount() - 1);
            kkvIter->MoveToNext();
        }
        kkvIter->Finish();
    }

    return true;
}

bool KkvLookupJoinKernel::multiJoin(ColumnPtr joinColumn, ValueType vt, size_t rowCount) {
    int64_t timestamp = TimeUtility::currentTime();
    auto deleter = [this](KKVIterator *p) { IE_POOL_COMPATIBLE_DELETE_CLASS(_pool, p); };
    unique_ptr<KKVIterator, decltype(deleter)> kkvIter(nullptr, deleter);

    for (size_t i = 0; i < rowCount; ++i) {
        switch (vt.getBuiltinType()) {
#define INT_CASE_CLAUSE(ft)                                             \
            case ft: {                                                  \
                typedef MatchDocBuiltinType2CppType<ft, true>::CppType T; \
                auto columnData = joinColumn->getColumnData<T>();       \
                if (unlikely(!columnData)) {                            \
                    SQL_LOG(ERROR, "impossible cast reference failed"); \
                    return false;                                       \
                }                                                       \
                T joinValues = columnData->get(i);                        \
                for (size_t k = 0; k < joinValues.size(); ++k) {          \
                    kkvIter.reset(_kkvReader->Lookup((PKeyType)joinValues[k], timestamp, tsc_default, _pool)); \
                    if (kkvIter == nullptr) {                           \
                        continue;                                       \
                    }                                                   \
                    while (kkvIter->IsValid()) {\
                        auto rightRow = _rightTable->allocateRow();     \
                        _valueCollectorPtr->collectValueFields(rightRow, kkvIter.get()); \
                        joinRow(i, _rightTable->getRowCount() - 1);     \
                        kkvIter->MoveToNext();                          \
                    }                                                   \
                    kkvIter->Finish();                                  \
                }                                                       \
                break;                                                  \
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
            MultiString joinValues = columnData->get(i);
            for (size_t k = 0; k < joinValues.size(); ++k) {
                ConstString key(joinValues[k].data(), joinValues[k].size(), _pool);
                kkvIter.reset(_kkvReader->Lookup(key, timestamp, tsc_default, _pool));
                if (kkvIter == nullptr) {
                    continue;
                }
                while (kkvIter->IsValid()) {
                    auto rightRow = _rightTable->allocateRow();
                    _valueCollectorPtr->collectValueFields(rightRow, kkvIter.get());
                    joinRow(i, _rightTable->getRowCount() - 1);
                    kkvIter->MoveToNext();
                }
                kkvIter->Finish();
            }
            break;
        }
        default: {
            // impossible
            SQL_LOG(ERROR, "impossible reach this branch");
            return false;
        }
        } // switch
    }

    return true;
}


REGISTER_KERNEL(KkvLookupJoinKernel);

END_HA3_NAMESPACE(sql);
