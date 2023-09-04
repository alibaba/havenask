#include "sql/ops/scan/Collector.h"

#include <assert.h>
#include <memory>
#include <stddef.h>
#include <string>

#include "autil/PackDataFormatter.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "indexlib/base/FieldType.h"
#include "indexlib/base/Status.h"
#include "indexlib/config/FileCompressSchema.h"
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/config/legacy_schema_adapter.h"
#include "indexlib/framework/TabletSchemaLoader.h"
#include "indexlib/index/attribute/Constant.h"
#include "indexlib/index/kv/Common.h"
#include "indexlib/index/kv/config/KVIndexConfig.h"
#include "indexlib/partition/index_partition.h"
#include "indexlib/partition/partition_group_resource.h"
#include "indexlib/table/BuiltinDefine.h"
#include "indexlib/testlib/indexlib_partition_creator.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/MountInfo.h"
#include "matchdoc/Reference.h"
#include "matchdoc/Trait.h"
#include "matchdoc/ValueType.h"
#include "navi/util/NaviTestPool.h"
#include "unittest/unittest.h"

using namespace matchdoc;
using namespace indexlib::partition;
using namespace indexlib::testlib;
using namespace indexlib::index;
using namespace indexlib::config;

namespace sql {

class CollectorTest : public TESTBASE {
private:
    void testDeclareRef(FieldType ft, bool isMulti, BuiltinType expect_bt) {
        MatchDocAllocatorPtr allocator(new MatchDocAllocator(&_pool));
        std::string valueName = "value";
        std::string groupName = "group";
        ReferenceBase *ref
            = CollectorUtil::declareReference(allocator, ft, isMulti, valueName, groupName);
        ASSERT_TRUE(ref);
        ASSERT_EQ(groupName, ref->getGroupName());
        ASSERT_EQ(valueName, ref->getName());
        ASSERT_EQ(isMulti, ref->getValueType().isMultiValue());
        ASSERT_EQ(expect_bt, ref->getValueType().getBuiltinType());
    }

    void testDeclareRefFailed(FieldType ft, bool isMulti) {
        MatchDocAllocatorPtr allocator(new MatchDocAllocator(&_pool));
        std::string valueName = "value";
        std::string groupName = "group";
        ReferenceBase *ref
            = CollectorUtil::declareReference(allocator, ft, isMulti, valueName, groupName);
        ASSERT_FALSE(ref);
    }

    std::shared_ptr<indexlibv2::config::ITabletSchema>
    convertToV2Schema(const std::shared_ptr<indexlibv2::config::ITabletSchema> &schema) {
        const auto &legacySchemaAdapter
            = std::dynamic_pointer_cast<indexlib::config::LegacySchemaAdapter>(schema);
        assert(legacySchemaAdapter);
        const auto &legacySchema = legacySchemaAdapter->GetLegacySchema();
        assert(legacySchema);
        auto jsonStr = autil::legacy::FastToJsonString(*legacySchema);
        auto tabletSchema = indexlibv2::framework::TabletSchemaLoader::LoadSchema(jsonStr);
        assert(tabletSchema);
        auto status = indexlibv2::framework::TabletSchemaLoader::ResolveSchema(
            nullptr, "", tabletSchema.get());
        assert(status.IsOK());
        return tabletSchema;
    }

    std::shared_ptr<indexlibv2::config::ITabletSchema> prepareKvSchema() {
        std::string tableName = "kvTable";
        std::string fields = "sk:int64;attr2:uint32;trigger_id:int64;name:string;value:string:true";
        std::string pkeyField = "trigger_id";
        std::string attributes = "sk;attr2;name;value";
        IndexPartitionSchemaPtr schema = IndexlibPartitionCreator::CreateKVSchema(
            tableName, fields, pkeyField, attributes, false);
        return std::make_shared<LegacySchemaAdapter>(schema);
    }

    std::shared_ptr<indexlibv2::config::ITabletSchema> prepareKvSchemaWithSingleValue() {
        std::string tableName = "kvTable";
        std::string fields = "sk:int64;attr2:uint32;trigger_id:int64;name:string;value:string:true";
        std::string pkeyField = "trigger_id";
        std::string attributes = "attr2";
        IndexPartitionSchemaPtr schema = IndexlibPartitionCreator::CreateKVSchema(
            tableName, fields, pkeyField, attributes, false);
        return std::make_shared<LegacySchemaAdapter>(schema);
    }

    std::shared_ptr<indexlibv2::config::ITabletSchema> prepareKvSchemaWithPkAttr() {
        std::string tableName = "kkvTable";
        std::string fields = "sk:int64;attr2:uint32;trigger_id:int64;name:string;value:string:true";
        std::string pkeyField = "trigger_id";
        std::string attributes = "trigger_id;sk;attr2;name;value";
        IndexPartitionSchemaPtr schema = IndexlibPartitionCreator::CreateKVSchema(
            tableName, fields, pkeyField, attributes, false);
        return std::make_shared<LegacySchemaAdapter>(schema);
    }

    std::shared_ptr<indexlibv2::config::ITabletSchema> prepareKkvSchema() {
        std::string tableName = "kkvTable";
        std::string fields = "sk:int64;attr2:uint32;trigger_id:int64;name:string;value:string:true";
        std::string pkeyField = "trigger_id";
        std::string skeyField = "sk";
        std::string attributes = "sk;attr2;name;value";
        IndexPartitionSchemaPtr schema = IndexlibPartitionCreator::CreateKKVSchema(
            tableName, fields, pkeyField, skeyField, attributes);
        return std::make_shared<LegacySchemaAdapter>(schema);
    }

    std::shared_ptr<indexlibv2::config::ITabletSchema> prepareKkvSchemaWithPkAttr() {
        std::string tableName = "kkvTable";
        std::string fields = "sk:int64;attr2:uint32;trigger_id:int64;name:string;value:string:true";
        std::string pkeyField = "trigger_id";
        std::string skeyField = "sk";
        std::string attributes = "trigger_id;sk;attr2;name;value";
        IndexPartitionSchemaPtr schema = IndexlibPartitionCreator::CreateKKVSchema(
            tableName, fields, pkeyField, skeyField, attributes);
        return std::make_shared<LegacySchemaAdapter>(schema);
    }

    std::shared_ptr<indexlibv2::config::ITabletSchema> prepareKkvSchemaWithStringSk() {
        std::string tableName = "kkvTable";
        std::string fields
            = "sk:string;attr2:uint32;trigger_id:int64;name:string;value:string:true";
        std::string pkeyField = "trigger_id";
        std::string skeyField = "sk";
        std::string attributes = "sk;attr2;name;value";
        IndexPartitionSchemaPtr schema = IndexlibPartitionCreator::CreateKKVSchema(
            tableName, fields, pkeyField, skeyField, attributes);
        return std::make_shared<LegacySchemaAdapter>(schema);
    }

private:
    autil::mem_pool::PoolAsan _pool;
};

// test declareReference()
TEST_F(CollectorTest, testDeclareReference) {
    testDeclareRef(ft_int8, false, bt_int8);
    testDeclareRef(ft_int16, false, bt_int16);
    testDeclareRef(ft_int32, false, bt_int32);
    testDeclareRef(ft_integer, false, bt_int32);
    testDeclareRef(ft_int64, false, bt_int64);
    testDeclareRef(ft_long, false, bt_int64);
    testDeclareRef(ft_uint8, false, bt_uint8);
    testDeclareRef(ft_uint16, false, bt_uint16);
    testDeclareRef(ft_uint32, false, bt_uint32);
    testDeclareRef(ft_uint64, false, bt_uint64);
    testDeclareRef(ft_float, false, bt_float);
    testDeclareRef(ft_double, false, bt_double);
    testDeclareRef(ft_string, false, bt_string);
    testDeclareRef(ft_hash_64, false, bt_uint64);
    testDeclareRef(ft_hash_128, false, bt_hash_128);

    testDeclareRef(ft_int8, true, bt_int8);
    testDeclareRef(ft_int16, true, bt_int16);
    testDeclareRef(ft_int32, true, bt_int32);
    testDeclareRef(ft_integer, true, bt_int32);
    testDeclareRef(ft_int64, true, bt_int64);
    testDeclareRef(ft_long, true, bt_int64);
    testDeclareRef(ft_uint8, true, bt_uint8);
    testDeclareRef(ft_uint16, true, bt_uint16);
    testDeclareRef(ft_uint32, true, bt_uint32);
    testDeclareRef(ft_uint64, true, bt_uint64);
    testDeclareRef(ft_float, true, bt_float);
    testDeclareRef(ft_double, true, bt_double);
    testDeclareRef(ft_string, true, bt_string);
    testDeclareRef(ft_hash_64, true, bt_uint64);
}

TEST_F(CollectorTest, testDeclareReferenceFailed) {
    testDeclareRefFailed(ft_text, false);
    testDeclareRefFailed(ft_enum, false);
    testDeclareRefFailed(ft_time, false);
    testDeclareRefFailed(ft_location, false);
    testDeclareRefFailed(ft_online, false);
    testDeclareRefFailed(ft_property, false);
    testDeclareRefFailed(ft_raw, false);
    testDeclareRefFailed(ft_unknown, false);

    testDeclareRefFailed(ft_text, true);
    testDeclareRefFailed(ft_enum, true);
    testDeclareRefFailed(ft_time, true);
    testDeclareRefFailed(ft_location, true);
    testDeclareRefFailed(ft_online, true);
    testDeclareRefFailed(ft_property, true);
    testDeclareRefFailed(ft_raw, true);
    testDeclareRefFailed(ft_unknown, true);
    testDeclareRefFailed(ft_hash_128, true);
}

// test init()
TEST_F(CollectorTest, testInitWithoutMount) {
    {
        std::shared_ptr<indexlibv2::config::ITabletSchema> schema = prepareKvSchema();
        MatchDocAllocatorPtr allocator(new MatchDocAllocator(&_pool));
        KeyCollector keyCollector;
        ValueCollector valueCollector;
        ASSERT_TRUE(keyCollector.init(schema, allocator));
        ASSERT_TRUE(NULL != keyCollector.getPkeyRef());
        ASSERT_EQ(bt_int64, keyCollector.getPkeyBuiltinType());
        auto legacySchemaAdapter
            = std::dynamic_pointer_cast<indexlib::config::LegacySchemaAdapter>(schema);
        ASSERT_TRUE(legacySchemaAdapter != nullptr);
        ASSERT_FALSE(valueCollector.init(legacySchemaAdapter->GetLegacySchema(), allocator));
        ASSERT_FALSE(valueCollector._skeyRef);
    }
    {
        std::shared_ptr<indexlibv2::config::ITabletSchema> schema
            = prepareKvSchemaWithSingleValue();
        MatchDocAllocatorPtr allocator(new MatchDocAllocator(&_pool));
        KeyCollector keyCollector;
        ValueCollector valueCollector;
        ASSERT_TRUE(keyCollector.init(schema, allocator));
        ASSERT_TRUE(NULL != keyCollector.getPkeyRef());
        ASSERT_EQ(bt_int64, keyCollector.getPkeyBuiltinType());
        auto legacySchemaAdapter
            = std::dynamic_pointer_cast<indexlib::config::LegacySchemaAdapter>(schema);
        ASSERT_TRUE(legacySchemaAdapter != nullptr);
        ASSERT_TRUE(valueCollector.init(legacySchemaAdapter->GetLegacySchema(), allocator));
        ASSERT_TRUE(1 == valueCollector._valueRefs.size());
        ASSERT_FALSE(valueCollector._skeyRef);
    }
    {
        std::shared_ptr<indexlibv2::config::ITabletSchema> schema = prepareKvSchemaWithPkAttr();
        MatchDocAllocatorPtr allocator(new MatchDocAllocator(&_pool));
        KeyCollector keyCollector;
        ValueCollector valueCollector;
        ASSERT_TRUE(keyCollector.init(schema, allocator));
        ASSERT_TRUE(NULL == keyCollector.getPkeyRef());
        ASSERT_EQ(bt_int64, keyCollector.getPkeyBuiltinType());
        auto legacySchemaAdapter
            = std::dynamic_pointer_cast<indexlib::config::LegacySchemaAdapter>(schema);
        ASSERT_TRUE(legacySchemaAdapter != nullptr);
        ASSERT_FALSE(valueCollector.init(legacySchemaAdapter->GetLegacySchema(), allocator));
        ASSERT_FALSE(valueCollector._skeyRef);
    }
    {
        std::shared_ptr<indexlibv2::config::ITabletSchema> schema = prepareKkvSchema();
        MatchDocAllocatorPtr allocator(new MatchDocAllocator(&_pool));
        KeyCollector keyCollector;
        ValueCollector valueCollector;
        ASSERT_TRUE(keyCollector.init(schema, allocator));
        ASSERT_TRUE(NULL != keyCollector.getPkeyRef());
        ASSERT_EQ(bt_int64, keyCollector.getPkeyBuiltinType());
        auto legacySchemaAdapter
            = std::dynamic_pointer_cast<indexlib::config::LegacySchemaAdapter>(schema);
        ASSERT_TRUE(legacySchemaAdapter != nullptr);
        ASSERT_FALSE(valueCollector.init(legacySchemaAdapter->GetLegacySchema(), allocator));
        ASSERT_TRUE(valueCollector._skeyRef);
    }
    {
        std::shared_ptr<indexlibv2::config::ITabletSchema> schema = prepareKkvSchemaWithPkAttr();
        MatchDocAllocatorPtr allocator(new MatchDocAllocator(&_pool));
        KeyCollector keyCollector;
        ValueCollector valueCollector;
        ASSERT_TRUE(keyCollector.init(schema, allocator));
        ASSERT_TRUE(NULL == keyCollector.getPkeyRef());
        ASSERT_EQ(bt_int64, keyCollector.getPkeyBuiltinType());
        auto legacySchemaAdapter
            = std::dynamic_pointer_cast<indexlib::config::LegacySchemaAdapter>(schema);
        ASSERT_TRUE(legacySchemaAdapter != nullptr);
        ASSERT_FALSE(valueCollector.init(legacySchemaAdapter->GetLegacySchema(), allocator));
        ASSERT_FALSE(valueCollector._skeyRef);
    }
}

TEST_F(CollectorTest, testInitWithoutMountV2Schema) {
    {
        std::shared_ptr<indexlibv2::config::ITabletSchema> schema
            = convertToV2Schema(prepareKvSchema());
        MatchDocAllocatorPtr allocator(new MatchDocAllocator(&_pool));
        KeyCollector keyCollector;
        ValueCollector valueCollector;
        ASSERT_TRUE(keyCollector.init(schema, allocator));
        ASSERT_TRUE(NULL != keyCollector.getPkeyRef());
        ASSERT_EQ(bt_int64, keyCollector.getPkeyBuiltinType());
        auto indexConfig = schema->GetIndexConfig(indexlib::index::KV_INDEX_TYPE_STR, "trigger_id");
        ASSERT_TRUE(indexConfig);
        auto kvIndexConfig
            = std::dynamic_pointer_cast<indexlibv2::config::KVIndexConfig>(indexConfig);
        ASSERT_TRUE(kvIndexConfig);
        ASSERT_FALSE(valueCollector.init(
            kvIndexConfig->GetValueConfig(), allocator, indexlib::table::TABLE_TYPE_KV));
        ASSERT_FALSE(valueCollector._skeyRef);
    }
    {
        std::shared_ptr<indexlibv2::config::ITabletSchema> schema
            = convertToV2Schema(prepareKvSchemaWithSingleValue());
        MatchDocAllocatorPtr allocator(new MatchDocAllocator(&_pool));
        KeyCollector keyCollector;
        ValueCollector valueCollector;

        ASSERT_TRUE(keyCollector.init(schema, allocator));
        ASSERT_TRUE(NULL != keyCollector.getPkeyRef());
        ASSERT_EQ(bt_int64, keyCollector.getPkeyBuiltinType());

        auto indexConfig = schema->GetIndexConfig(indexlib::index::KV_INDEX_TYPE_STR, "trigger_id");
        ASSERT_TRUE(indexConfig);
        auto kvIndexConfig
            = std::dynamic_pointer_cast<indexlibv2::config::KVIndexConfig>(indexConfig);
        ASSERT_TRUE(kvIndexConfig);
        ASSERT_TRUE(valueCollector.init(
            kvIndexConfig->GetValueConfig(), allocator, indexlib::table::TABLE_TYPE_KV));
        ASSERT_TRUE(1 == valueCollector._valueRefs.size());
        ASSERT_FALSE(valueCollector._skeyRef);
    }
    {
        std::shared_ptr<indexlibv2::config::ITabletSchema> schema
            = convertToV2Schema(prepareKvSchemaWithPkAttr());
        MatchDocAllocatorPtr allocator(new MatchDocAllocator(&_pool));
        KeyCollector keyCollector;
        ValueCollector valueCollector;
        ASSERT_TRUE(keyCollector.init(schema, allocator));
        ASSERT_TRUE(NULL == keyCollector.getPkeyRef());
        ASSERT_EQ(bt_int64, keyCollector.getPkeyBuiltinType());
        auto indexConfig = schema->GetIndexConfig(indexlib::index::KV_INDEX_TYPE_STR, "trigger_id");
        ASSERT_TRUE(indexConfig);
        auto kvIndexConfig
            = std::dynamic_pointer_cast<indexlibv2::config::KVIndexConfig>(indexConfig);
        ASSERT_TRUE(kvIndexConfig);
        ASSERT_FALSE(valueCollector.init(
            kvIndexConfig->GetValueConfig(), allocator, indexlib::table::TABLE_TYPE_KV));
        ASSERT_FALSE(valueCollector._skeyRef);
    }
    // not support kkv v2 now
    // {
    //     std::shared_ptr<indexlibv2::config::ITabletSchema> schema =
    //     convertToV2Schema(prepareKkvSchema()); MatchDocAllocatorPtr allocator(new
    //     MatchDocAllocator(&_pool)); KeyCollector keyCollector; ValueCollector valueCollector;
    //     ASSERT_TRUE(keyCollector.init(schema, allocator));
    //     ASSERT_TRUE(NULL != keyCollector.getPkeyRef());
    //     ASSERT_EQ(bt_int64, keyCollector.getPkeyBuiltinType());
    //     auto indexConfig = schema->GetIndexConfig(indexlib::index::KV_INDEX_TYPE_STR,
    //     "trigger_id"); ASSERT_TRUE(indexConfig); auto kvIndexConfig =
    //     std::dynamic_pointer_cast<indexlibv2::config::KVIndexConfig>(indexConfig);
    //     ASSERT_TRUE(kvIndexConfig);
    //     ASSERT_FALSE(valueCollector.init(kvIndexConfig->GetValueConfig(), allocator,
    //     indexlib::table::TABLE_TYPE_KV)); ASSERT_FALSE(valueCollector._skeyRef);
    // }
    // {
    //     std::shared_ptr<indexlibv2::config::ITabletSchema> schema =
    //     convertToV2Schema(prepareKkvSchemaWithPkAttr()); MatchDocAllocatorPtr allocator(new
    //     MatchDocAllocator(&_pool)); KeyCollector keyCollector; ValueCollector valueCollector;
    //     ASSERT_TRUE(keyCollector.init(schema, allocator));
    //     ASSERT_TRUE(NULL == keyCollector.getPkeyRef());
    //     ASSERT_EQ(bt_int64, keyCollector.getPkeyBuiltinType());
    //     ASSERT_FALSE(valueCollector.init(schema, allocator));
    //     ASSERT_FALSE(valueCollector._skeyRef);
    // }
}

TEST_F(CollectorTest, testInitWithMount) {
    {
        std::shared_ptr<indexlibv2::config::ITabletSchema> schema = prepareKvSchema();
        MountInfoPtr mountInfo = CollectorUtil::createPackAttributeMountInfo(schema);
        MatchDocAllocatorPtr allocator(new MatchDocAllocator(&_pool, false, mountInfo));
        ValueCollector valueCollector;
        auto legacySchemaAdapter
            = std::dynamic_pointer_cast<indexlib::config::LegacySchemaAdapter>(schema);
        ASSERT_TRUE(legacySchemaAdapter != nullptr);
        bool ret = valueCollector.init(legacySchemaAdapter->GetLegacySchema(), allocator);
        ASSERT_TRUE(ret);
        ASSERT_EQ(4, valueCollector._valueRefs.size());
        ASSERT_EQ("sk", valueCollector._valueRefs[0]->getName());
        ASSERT_EQ("attr2", valueCollector._valueRefs[1]->getName());
        ASSERT_TRUE(valueCollector._valueRefs[1]->isMount());
        ASSERT_EQ("name", valueCollector._valueRefs[2]->getName());
        ASSERT_EQ("value", valueCollector._valueRefs[3]->getName());
        ASSERT_FALSE(valueCollector._skeyRef);
    }
    {
        std::shared_ptr<indexlibv2::config::ITabletSchema> schema
            = prepareKvSchemaWithSingleValue();
        MountInfoPtr mountInfo = CollectorUtil::createPackAttributeMountInfo(schema);
        MatchDocAllocatorPtr allocator(new MatchDocAllocator(&_pool, false, mountInfo));
        ValueCollector valueCollector;
        auto legacySchemaAdapter
            = std::dynamic_pointer_cast<indexlib::config::LegacySchemaAdapter>(schema);
        ASSERT_TRUE(legacySchemaAdapter != nullptr);
        bool ret = valueCollector.init(legacySchemaAdapter->GetLegacySchema(), allocator);
        ASSERT_TRUE(ret);
        ASSERT_TRUE(1 == valueCollector._valueRefs.size());
        ASSERT_FALSE(valueCollector._valueRefs[0]->isMount());
        ASSERT_FALSE(valueCollector._skeyRef);
    }
    {
        std::shared_ptr<indexlibv2::config::ITabletSchema> schema = prepareKvSchemaWithPkAttr();
        MountInfoPtr mountInfo = CollectorUtil::createPackAttributeMountInfo(schema);
        MatchDocAllocatorPtr allocator(new MatchDocAllocator(&_pool, false, mountInfo));
        ValueCollector valueCollector;
        auto legacySchemaAdapter
            = std::dynamic_pointer_cast<indexlib::config::LegacySchemaAdapter>(schema);
        ASSERT_TRUE(legacySchemaAdapter != nullptr);
        bool ret = valueCollector.init(legacySchemaAdapter->GetLegacySchema(), allocator);
        ASSERT_TRUE(ret);
        ASSERT_EQ(5, valueCollector._valueRefs.size());
        ASSERT_EQ("trigger_id", valueCollector._valueRefs[0]->getName());
        ASSERT_EQ("sk", valueCollector._valueRefs[1]->getName());
        ASSERT_EQ("attr2", valueCollector._valueRefs[2]->getName());
        ASSERT_TRUE(valueCollector._valueRefs[2]->isMount());
        ASSERT_EQ("name", valueCollector._valueRefs[3]->getName());
        ASSERT_EQ("value", valueCollector._valueRefs[4]->getName());
        ASSERT_FALSE(valueCollector._skeyRef);
    }
    {
        std::shared_ptr<indexlibv2::config::ITabletSchema> schema = prepareKkvSchema();
        MountInfoPtr mountInfo = CollectorUtil::createPackAttributeMountInfo(schema);
        MatchDocAllocatorPtr allocator(new MatchDocAllocator(&_pool, false, mountInfo));
        ValueCollector valueCollector;
        auto legacySchemaAdapter
            = std::dynamic_pointer_cast<indexlib::config::LegacySchemaAdapter>(schema);
        ASSERT_TRUE(legacySchemaAdapter != nullptr);
        bool ret = valueCollector.init(legacySchemaAdapter->GetLegacySchema(), allocator);
        ASSERT_TRUE(ret);
        ASSERT_EQ(3, valueCollector._valueRefs.size());
        ASSERT_EQ("attr2", valueCollector._valueRefs[0]->getName());
        ASSERT_TRUE(valueCollector._valueRefs[0]->isMount());
        ASSERT_EQ("name", valueCollector._valueRefs[1]->getName());
        ASSERT_EQ("value", valueCollector._valueRefs[2]->getName());
        ASSERT_TRUE(valueCollector._skeyRef);
        ASSERT_EQ("sk", valueCollector._skeyRef->getName());
        ASSERT_FALSE(valueCollector._skeyRef->isMount());
    }
    {
        std::shared_ptr<indexlibv2::config::ITabletSchema> schema = prepareKkvSchemaWithPkAttr();
        MountInfoPtr mountInfo = CollectorUtil::createPackAttributeMountInfo(schema);
        MatchDocAllocatorPtr allocator(new MatchDocAllocator(&_pool, false, mountInfo));
        ValueCollector valueCollector;
        auto legacySchemaAdapter
            = std::dynamic_pointer_cast<indexlib::config::LegacySchemaAdapter>(schema);
        ASSERT_TRUE(legacySchemaAdapter != nullptr);
        bool ret = valueCollector.init(legacySchemaAdapter->GetLegacySchema(), allocator);

        ASSERT_TRUE(ret);
        ASSERT_EQ(4, valueCollector._valueRefs.size());
        ASSERT_EQ("trigger_id", valueCollector._valueRefs[0]->getName());
        ASSERT_TRUE(valueCollector._valueRefs[0]->isMount());
        ASSERT_EQ("attr2", valueCollector._valueRefs[1]->getName());
        ASSERT_EQ("name", valueCollector._valueRefs[2]->getName());
        ASSERT_EQ("value", valueCollector._valueRefs[3]->getName());
        ASSERT_TRUE(valueCollector._skeyRef);
        ASSERT_EQ("sk", valueCollector._skeyRef->getName());
    }
    {
        std::shared_ptr<indexlibv2::config::ITabletSchema> schema = prepareKkvSchemaWithStringSk();
        MountInfoPtr mountInfo = CollectorUtil::createPackAttributeMountInfo(schema);
        MatchDocAllocatorPtr allocator(new MatchDocAllocator(&_pool, false, mountInfo));
        ValueCollector valueCollector;
        auto legacySchemaAdapter
            = std::dynamic_pointer_cast<indexlib::config::LegacySchemaAdapter>(schema);
        ASSERT_TRUE(legacySchemaAdapter != nullptr);
        bool ret = valueCollector.init(legacySchemaAdapter->GetLegacySchema(), allocator);
        ASSERT_TRUE(ret);
        ASSERT_EQ(4, valueCollector._valueRefs.size());
        ASSERT_EQ("sk", valueCollector._valueRefs[0]->getName());
        ASSERT_EQ("attr2", valueCollector._valueRefs[1]->getName());
        ASSERT_TRUE(valueCollector._valueRefs[1]->isMount());
        ASSERT_EQ("name", valueCollector._valueRefs[2]->getName());
        ASSERT_EQ("value", valueCollector._valueRefs[3]->getName());
        ASSERT_FALSE(valueCollector._skeyRef);
    }
}

TEST_F(CollectorTest, testInitWithMountV2Schema) {
    {
        std::shared_ptr<indexlibv2::config::ITabletSchema> schema
            = convertToV2Schema(prepareKvSchema());
        MountInfoPtr mountInfo = CollectorUtil::createPackAttributeMountInfo(schema);
        MatchDocAllocatorPtr allocator(new MatchDocAllocator(&_pool, false, mountInfo));
        ValueCollector valueCollector;

        auto indexConfig = schema->GetIndexConfig(indexlib::index::KV_INDEX_TYPE_STR, "trigger_id");
        ASSERT_TRUE(indexConfig);
        auto kvIndexConfig
            = std::dynamic_pointer_cast<indexlibv2::config::KVIndexConfig>(indexConfig);
        ASSERT_TRUE(kvIndexConfig);
        bool ret = valueCollector.init(
            kvIndexConfig->GetValueConfig(), allocator, indexlib::table::TABLE_TYPE_KV);
        ASSERT_TRUE(ret);
        ASSERT_EQ(4, valueCollector._valueRefs.size());
        ASSERT_EQ("sk", valueCollector._valueRefs[0]->getName());
        ASSERT_EQ("attr2", valueCollector._valueRefs[1]->getName());
        ASSERT_TRUE(valueCollector._valueRefs[1]->isMount());
        ASSERT_EQ("name", valueCollector._valueRefs[2]->getName());
        ASSERT_EQ("value", valueCollector._valueRefs[3]->getName());
        ASSERT_FALSE(valueCollector._skeyRef);
    }
    {
        std::shared_ptr<indexlibv2::config::ITabletSchema> schema
            = convertToV2Schema(prepareKvSchemaWithSingleValue());
        MountInfoPtr mountInfo = CollectorUtil::createPackAttributeMountInfo(schema);
        MatchDocAllocatorPtr allocator(new MatchDocAllocator(&_pool, false, mountInfo));
        ValueCollector valueCollector;
        auto indexConfig = schema->GetIndexConfig(indexlib::index::KV_INDEX_TYPE_STR, "trigger_id");
        ASSERT_TRUE(indexConfig);
        auto kvIndexConfig
            = std::dynamic_pointer_cast<indexlibv2::config::KVIndexConfig>(indexConfig);
        ASSERT_TRUE(kvIndexConfig);
        bool ret = valueCollector.init(
            kvIndexConfig->GetValueConfig(), allocator, indexlib::table::TABLE_TYPE_KV);
        ASSERT_TRUE(ret);
        ASSERT_TRUE(1 == valueCollector._valueRefs.size());
        ASSERT_FALSE(valueCollector._valueRefs[0]->isMount());
        ASSERT_FALSE(valueCollector._skeyRef);
    }
    {
        std::shared_ptr<indexlibv2::config::ITabletSchema> schema
            = convertToV2Schema(prepareKvSchemaWithPkAttr());
        MountInfoPtr mountInfo = CollectorUtil::createPackAttributeMountInfo(schema);
        MatchDocAllocatorPtr allocator(new MatchDocAllocator(&_pool, false, mountInfo));
        ValueCollector valueCollector;
        auto indexConfig = schema->GetIndexConfig(indexlib::index::KV_INDEX_TYPE_STR, "trigger_id");
        ASSERT_TRUE(indexConfig);
        auto kvIndexConfig
            = std::dynamic_pointer_cast<indexlibv2::config::KVIndexConfig>(indexConfig);
        ASSERT_TRUE(kvIndexConfig);
        bool ret = valueCollector.init(
            kvIndexConfig->GetValueConfig(), allocator, indexlib::table::TABLE_TYPE_KV);
        ASSERT_TRUE(ret);
        ASSERT_EQ(5, valueCollector._valueRefs.size());
        ASSERT_EQ("trigger_id", valueCollector._valueRefs[0]->getName());
        ASSERT_EQ("sk", valueCollector._valueRefs[1]->getName());
        ASSERT_EQ("attr2", valueCollector._valueRefs[2]->getName());
        ASSERT_TRUE(valueCollector._valueRefs[2]->isMount());
        ASSERT_EQ("name", valueCollector._valueRefs[3]->getName());
        ASSERT_EQ("value", valueCollector._valueRefs[4]->getName());
        ASSERT_FALSE(valueCollector._skeyRef);
    }
    //     {
    //         std::shared_ptr<indexlibv2::config::ITabletSchema> schema =
    //         convertToV2Schema(prepareKkvSchema()); MountInfoPtr mountInfo =
    //         CollectorUtil::createPackAttributeMountInfo(schema); MatchDocAllocatorPtr
    //         allocator(new MatchDocAllocator(&_pool, false, mountInfo)); ValueCollector
    //         valueCollector; bool ret = valueCollector.init(schema, allocator); ASSERT_TRUE(ret);
    //         ASSERT_EQ(3, valueCollector._valueRefs.size());
    //         ASSERT_EQ("attr2", valueCollector._valueRefs[0]->getName());
    //         ASSERT_TRUE(valueCollector._valueRefs[0]->isMount());
    //         ASSERT_EQ("name", valueCollector._valueRefs[1]->getName());
    //         ASSERT_EQ("value", valueCollector._valueRefs[2]->getName());
    //         ASSERT_TRUE(valueCollector._skeyRef);
    //         ASSERT_EQ("sk", valueCollector._skeyRef->getName());
    //         ASSERT_FALSE(valueCollector._skeyRef->isMount());
    //     }
    //     {
    //         std::shared_ptr<indexlibv2::config::ITabletSchema> schema =
    //         convertToV2Schema(prepareKkvSchemaWithPkAttr()); MountInfoPtr mountInfo =
    //         CollectorUtil::createPackAttributeMountInfo(schema); MatchDocAllocatorPtr
    //         allocator(new MatchDocAllocator(&_pool, false, mountInfo)); ValueCollector
    //         valueCollector; bool ret = valueCollector.init(schema, allocator); ASSERT_TRUE(ret);
    //         ASSERT_EQ(4, valueCollector._valueRefs.size());
    //         ASSERT_EQ("trigger_id", valueCollector._valueRefs[0]->getName());
    //         ASSERT_TRUE(valueCollector._valueRefs[0]->isMount());
    //         ASSERT_EQ("attr2", valueCollector._valueRefs[1]->getName());
    //         ASSERT_EQ("name", valueCollector._valueRefs[2]->getName());
    //         ASSERT_EQ("value", valueCollector._valueRefs[3]->getName());
    //         ASSERT_TRUE(valueCollector._skeyRef);
    //         ASSERT_EQ("sk", valueCollector._skeyRef->getName());
    //     }
    //     {
    //         std::shared_ptr<indexlibv2::config::ITabletSchema> schema =
    //         convertToV2Schema(prepareKkvSchemaWithStringSk()); MountInfoPtr mountInfo =
    //         CollectorUtil::createPackAttributeMountInfo(schema); MatchDocAllocatorPtr
    //         allocator(new MatchDocAllocator(&_pool, false, mountInfo)); ValueCollector
    //         valueCollector; bool ret = valueCollector.init(schema, allocator); ASSERT_TRUE(ret);
    //         ASSERT_EQ(4, valueCollector._valueRefs.size());
    //         ASSERT_EQ("sk", valueCollector._valueRefs[0]->getName());
    //         ASSERT_EQ("attr2", valueCollector._valueRefs[1]->getName());
    //         ASSERT_TRUE(valueCollector._valueRefs[1]->isMount());
    //         ASSERT_EQ("name", valueCollector._valueRefs[2]->getName());
    //         ASSERT_EQ("value", valueCollector._valueRefs[3]->getName());
    //         ASSERT_FALSE(valueCollector._skeyRef);
    //     }
}

TEST_F(CollectorTest, testInitWithSomeMount) {
    {
        std::shared_ptr<indexlibv2::config::ITabletSchema> schema = prepareKkvSchema();
        MountInfoPtr mountInfo = CollectorUtil::createPackAttributeMountInfo(schema);
        ASSERT_TRUE(mountInfo->erase("name"));
        ASSERT_TRUE(mountInfo->erase("kkvTable:name"));
        MatchDocAllocatorPtr allocator(new MatchDocAllocator(&_pool, false, mountInfo));
        ValueCollector valueCollector;
        auto legacySchemaAdapter
            = std::dynamic_pointer_cast<indexlib::config::LegacySchemaAdapter>(schema);
        ASSERT_TRUE(legacySchemaAdapter != nullptr);
        bool ret = valueCollector.init(legacySchemaAdapter->GetLegacySchema(), allocator);
        ASSERT_FALSE(ret);
        ASSERT_EQ(1, valueCollector._valueRefs.size()); // only attr2, return before value
        ASSERT_EQ("attr2", valueCollector._valueRefs[0]->getName());
        ASSERT_TRUE(valueCollector._skeyRef);
        ASSERT_EQ("sk", valueCollector._skeyRef->getName());
    }
}

// kv&kkv collectFields(...) TODO：在kvscan、kkvscan中测试

// test createMountInfo()
TEST_F(CollectorTest, testCreateMountInfo) {
    std::shared_ptr<indexlibv2::config::ITabletSchema> schema = prepareKkvSchema();
    MountInfoPtr mountInfo = CollectorUtil::createPackAttributeMountInfo(schema);
    ASSERT_TRUE(mountInfo.get());
    ASSERT_EQ(6, mountInfo->_mountMap.size());
    ASSERT_EQ(0, mountInfo->get("attr2")->mountId);
    ASSERT_EQ(autil::PackOffset::normalOffset(0, false).toUInt64(),
              mountInfo->get("attr2")->mountOffset);
    ASSERT_EQ(0, mountInfo->get("kkvTable:attr2")->mountId);
    ASSERT_EQ(autil::PackOffset::normalOffset(0, false).toUInt64(),
              mountInfo->get("kkvTable:attr2")->mountOffset);

    ASSERT_EQ(0, mountInfo->get("name")->mountId);
    ASSERT_EQ(autil::PackOffset::normalOffset(4, true).toUInt64(),
              mountInfo->get("name")->mountOffset);
    ASSERT_EQ(0, mountInfo->get("kkvTable:name")->mountId);
    ASSERT_EQ(autil::PackOffset::normalOffset(4, true).toUInt64(),
              mountInfo->get("kkvTable:name")->mountOffset);

    ASSERT_EQ(0, mountInfo->get("value")->mountId);
    ASSERT_EQ(autil::PackOffset::normalOffset(12, true).toUInt64(),
              mountInfo->get("value")->mountOffset);
    ASSERT_EQ(0, mountInfo->get("kkvTable:value")->mountId);
    ASSERT_EQ(autil::PackOffset::normalOffset(12, true).toUInt64(),
              mountInfo->get("kkvTable:value")->mountOffset);
}

} // namespace sql
