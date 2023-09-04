#include "suez/table/SuezIndexPartition.h"

#include <assert.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <iosfwd>
#include <memory>
#include <string>
#include <vector>

#include "MockTableBuilder.h"
#include "autil/legacy/exception.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/config/legacy_schema_adapter.h"
#include "indexlib/framework/TabletSchemaLoader.h"
#include "indexlib/framework/mock/MockTablet.h"
#include "indexlib/partition/index_partition.h"
#include "indexlib/testlib/indexlib_partition_creator.h"
#include "indexlib/testlib/mock_index_partition.h"
#include "suez/common/TableMeta.h"
#include "suez/common/TablePathDefine.h"
#include "suez/common/test/TableMetaUtil.h"
#include "suez/drc/LogReplicator.h"
#include "suez/sdk/TableWriter.h"
#include "suez/table/DummyTableBuilder.h"
#include "suez/table/IndexPartitionAdapter.h"
#include "suez/table/TabletAdapter.h"
#include "suez/table/test/MockSuezPartition.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace autil;
using namespace indexlib::testlib;
using namespace indexlib::partition;

namespace suez {

class SuezIndexPartitionTest : public TESTBASE {
public:
    void setUp() override {
        _resource = TableResource(TableMetaUtil::makePid("t1"));
        _resource.globalIndexResource = PartitionGroupResource::TEST_Create();
        _current = std::make_shared<CurrentPartitionMeta>();
    }

protected:
    indexlib::config::IndexPartitionSchemaPtr makeSchema(const string &fieldInfos,
                                                         const string &indexInfos,
                                                         const string &attrInfos,
                                                         const string &summaryInfos = "") {
        return IndexlibPartitionCreator::CreateSchema("test_table", fieldInfos, indexInfos, attrInfos, summaryInfos);
    }

    void checkUnloadedStatus() const {
        ASSERT_FALSE(_current->getFullIndexLoaded());
        ASSERT_TRUE(_current->getLoadedIndexRoot().empty());
        ASSERT_TRUE(_current->getLoadedConfigPath().empty());
        ASSERT_EQ(-1, _current->getIncVersion());
    }

protected:
    TableResource _resource;
    CurrentPartitionMetaPtr _current;
};

TEST_F(SuezIndexPartitionTest, testConstructEffectiveFieldInfo) {
    auto schema = makeSchema("id:int64;buyer_age:uint32;attr_str:string;", "id:primarykey64:id", "buyer_age;attr_str");
    auto effectiveFieldInfo = SuezIndexPartition::constructEffectiveFieldInfo(
        std::make_shared<indexlib::config::LegacySchemaAdapter>(schema));
    ASSERT_EQ(effectiveFieldInfo.attributes.size(), 2);
    ASSERT_EQ(effectiveFieldInfo.indexes.size(), 1);
    ASSERT_EQ(effectiveFieldInfo.attributes[0], "attr_str");
    ASSERT_EQ(effectiveFieldInfo.attributes[1], "buyer_age");
    ASSERT_EQ(effectiveFieldInfo.indexes[0], "id");
}

TEST_F(SuezIndexPartitionTest, testConstructEffectiveFieldInfoKKV) {
    string jsonString = R"(
    {
        "table_name": "kkv_table",
        "table_type": "kkv",
        "fields": [
        { "field_name": "nick", "field_type": "STRING" },
        { "field_name": "nid", "field_type": "INT64" }
        ],
        "indexs": [
        {
            "index_name": "nick_nid", "index_type": "PRIMARY_KEY",
            "index_fields": [
                {"field_name":"nick", "key_type":"prefix"},
                {"field_name":"nid", "key_type":"suffix", "count_limits":5000, "skip_list_threshold":200 }
            ],
            "index_preference": {
                "type" : "PERF",
                "parameters" : {
                    "hash_dict" : {},
                    "suffix_key" : { "encode" : false },
                    "value" : { "encode" : true, "file_compressor" : "lz4" }
                }
            }
        }
        ],
        "attributes": [ "nick", "nid" ]
    }
    )";
    indexlib::config::IndexPartitionSchemaPtr schema(new indexlib::config::IndexPartitionSchema(""));
    FromJson(*schema, autil::legacy::json::ParseJson(jsonString));
    ASSERT_EQ("kkv_table", schema->GetSchemaName());
    auto effectiveFieldInfo = SuezIndexPartition::constructEffectiveFieldInfo(
        std::make_shared<indexlib::config::LegacySchemaAdapter>(schema));
    ASSERT_EQ(effectiveFieldInfo.attributes.size(), 2);
    ASSERT_EQ(effectiveFieldInfo.indexes.size(), 1);
    ASSERT_EQ("nick", effectiveFieldInfo.attributes[0]);
    ASSERT_EQ("nid", effectiveFieldInfo.attributes[1]);
    ASSERT_EQ("nick_nid", effectiveFieldInfo.indexes[0]);
}

TEST_F(SuezIndexPartitionTest, testEmptyAttributes) {
    auto schema = makeSchema("id:int64;buyer_age:uint32;attr_str:string;", "id:primarykey64:id", "");
    auto effectiveFieldInfo = SuezIndexPartition::constructEffectiveFieldInfo(
        std::make_shared<indexlib::config::LegacySchemaAdapter>(schema));
    ASSERT_EQ(effectiveFieldInfo.attributes.size(), 0);
    ASSERT_EQ(effectiveFieldInfo.indexes.size(), 1);
    ASSERT_EQ(effectiveFieldInfo.indexes[0], "id");
}

TEST_F(SuezIndexPartitionTest, testDeploy) {
    auto target = TableMetaUtil::makeTarget(2, "t1_index/0/", "/t1_config/0");
    {
        NiceMockSuezIndexPartition p(_resource, _current);

        // deploy config failed
        EXPECT_CALL(p, doDeployConfig("/t1_config/0", _)).WillOnce(Return(DS_FAILED));
        EXPECT_CALL(p, doDeployIndex(_, _)).Times(0);
        auto ret = p.deploy(target, false);
        ASSERT_EQ(DS_FAILED, ret.status);
        ASSERT_EQ(DEPLOY_CONFIG_ERROR, ret.error);
        ASSERT_TRUE(_current->deployMeta->deployStatusMap.empty());
        ASSERT_TRUE(_current->getIndexRoot().empty());
        ASSERT_TRUE(_current->getConfigPath().empty());
    }

    {
        NiceMockSuezIndexPartition p(_resource, _current);

        // deploy index failed
        EXPECT_CALL(p, doDeployConfig("/t1_config/0", _)).WillOnce(Return(DS_DEPLOYDONE));
        EXPECT_CALL(p, doDeployIndex(_, _)).WillOnce(Return(DS_DISKQUOTA));
        auto ret = p.deploy(target, false);
        ASSERT_EQ(DS_DISKQUOTA, ret.status);
        ASSERT_EQ(DEPLOY_DISK_QUOTA_ERROR, ret.error);
        ASSERT_TRUE(_current->deployMeta->deployStatusMap.empty());
        ASSERT_TRUE(_current->getIndexRoot().empty());
        ASSERT_TRUE(_current->getConfigPath().empty());
    }
    {
        NiceMockSuezIndexPartition p(_resource, _current);

        // deploy done
        EXPECT_CALL(p, doDeployConfig("/t1_config/0", _)).WillOnce(Return(DS_DEPLOYDONE));
        EXPECT_CALL(p, doDeployIndex(_, _)).WillOnce(Return(DS_DEPLOYDONE));

        auto ret = p.deploy(target, false);
        ASSERT_EQ(DS_DEPLOYDONE, ret.status);
        ASSERT_EQ(ERROR_NONE, ret.error);

        ASSERT_EQ(DS_UNKNOWN, _current->getDeployStatus(1)); // weird, update in Table::deploy
        ASSERT_EQ(target.getIndexRoot(), _current->getIndexRoot());
        ASSERT_EQ(target.getConfigPath(), _current->getConfigPath());
    }
    {
        NiceMockSuezIndexPartition p(_resource, _current);
        _current->setIncVersion(1);
        _current->setConfigPath("/non-existed-config-path");

        EXPECT_CALL(p, doDeployConfig(_, _)).WillOnce(Return(DS_DEPLOYDONE));
        EXPECT_CALL(p, loadTableConfig(_, _)).WillOnce(Return(true)).WillOnce(Return(false));
        ON_CALL(p, doDeployIndex(_, _)).WillByDefault([&p](const TargetPartitionMeta &target, bool distDeploy) {
            return p.SuezIndexPartition::doDeployIndex(target, distDeploy);
        });
        p.getIndexDeployer()->setStatus(DS_DEPLOYDONE);
        auto ret = p.deploy(target, false);
        ASSERT_EQ(-1, p.getIndexDeployer()->getOldVersionId());
        ASSERT_EQ(2, p.getIndexDeployer()->getNewVersionId());
        ASSERT_EQ(DS_DEPLOYDONE, ret.status);
    }
    {
        NiceMockSuezIndexPartition p(_resource, _current);
        _current->setIncVersion(1);

        EXPECT_CALL(p, doDeployConfig(_, _)).WillOnce(Return(DS_DEPLOYDONE));
        EXPECT_CALL(p, loadTableConfig(_, _)).WillOnce(Return(true));
        ON_CALL(p, doDeployIndex(_, _)).WillByDefault([&p](const TargetPartitionMeta &target, bool distDeploy) {
            return p.SuezIndexPartition::doDeployIndex(target, distDeploy);
        });
        p.getIndexDeployer()->setStatus(DS_DEPLOYDONE);
        auto ret = p.deploy(target, false);
        ASSERT_EQ(1, p.getIndexDeployer()->getOldVersionId());
        ASSERT_EQ(2, p.getIndexDeployer()->getNewVersionId());
        ASSERT_EQ(DS_DEPLOYDONE, ret.status);
    }
    {
        NiceMockSuezIndexPartition p(_resource, _current);
        _current->setIncVersion(1);

        EXPECT_CALL(p, doDeployConfig(_, _)).WillOnce(Return(DS_DEPLOYDONE));
        EXPECT_CALL(p, loadTableConfig(_, _)).WillOnce(Return(true));
        ON_CALL(p, doDeployIndex(_, _)).WillByDefault([&p](const TargetPartitionMeta &target, bool distDeploy) {
            return p.SuezIndexPartition::doDeployIndex(target, distDeploy);
        });
        p.getIndexDeployer()->setStatus(DS_DEPLOYDONE);
        auto ret = p.deploy(target, true);
        ASSERT_EQ(-1, p.getIndexDeployer()->getOldVersionId());
        ASSERT_EQ(2, p.getIndexDeployer()->getNewVersionId());
        ASSERT_EQ(DS_DEPLOYDONE, ret.status);
    }
}

TEST_F(SuezIndexPartitionTest, testLoadUnload) {
    auto schema = makeSchema("id:int64;buyer_age:uint32;attr_str:string;", "id:primarykey64:id", "buyer_age;attr_str");

    auto target = TableMetaUtil::makeTarget(1, "t1_index/0/", "/t1_config/0");
    NiceMockSuezIndexPartition p(_resource, _current);

    EXPECT_CALL(p, doDeployConfig("/t1_config/0", _)).WillOnce(Return(DS_DEPLOYDONE));
    EXPECT_CALL(p, doDeployIndex(_, _)).WillOnce(Return(DS_DEPLOYDONE));

    auto ret = p.deploy(target, false);
    ASSERT_EQ(DS_DEPLOYDONE, ret.status);
    _current->setDeployStatus(1, DS_DEPLOYDONE); // TODO: move update status to partition

    EXPECT_CALL(p, loadTableConfig(_, _)).WillRepeatedly(Return(true));
    auto indexPart = MockIndexPartition::MakeNice();
    IndexPartitionPtr partPtr(indexPart);
    auto reader = new NiceMockIndexPartitionReader(schema);
    IndexPartitionReaderPtr readerPtr(reader);

    // create index partition failed
    EXPECT_CALL(p, createIndexlibAdapter(_, _)).WillOnce(Return(ByMove(std::unique_ptr<IIndexlibAdapter>())));
    EXPECT_CALL(*indexPart, Open(_, _, _, _, 1)).Times(0);
    auto ret2 = p.load(target, false);
    ASSERT_EQ(TS_ERROR_UNKNOWN, ret2.status);
    ASSERT_EQ(ERROR_NONE, ret2.error);
    ASSERT_FALSE(p._indexlibAdapter);
    checkUnloadedStatus();

    // open failed
    auto indexPartAdapter = std::make_unique<IndexPartitionAdapter>("", "", partPtr);
    EXPECT_CALL(p, createIndexlibAdapter(_, _)).WillOnce(Return(ByMove(std::move(indexPartAdapter))));
    EXPECT_CALL(*indexPart, Open(_, _, _, _, 1)).WillOnce(Return(IndexPartition::OS_FILEIO_EXCEPTION));

    ret2 = p.load(target, false);
    ASSERT_EQ(TS_FORCE_RELOAD, ret2.status);
    ASSERT_EQ(ERROR_NONE, ret2.error);
    checkUnloadedStatus();
    ASSERT_FALSE(p._indexlibAdapter);

    auto data = p.getPartitionData();
    ASSERT_FALSE(data);
    ASSERT_FALSE(p.isInUse());

    // loaded
    auto indexPartAdapter1 = std::make_unique<IndexPartitionAdapter>("", "", partPtr);
    EXPECT_CALL(p, createIndexlibAdapter(_, _)).WillOnce(Return(ByMove(std::move(indexPartAdapter1))));

    EXPECT_CALL(*indexPart, Open(_, _, _, _, 1)).WillOnce(Return(IndexPartition::OS_OK));
    EXPECT_CALL(*indexPart, GetReader()).WillRepeatedly(Return(readerPtr));
    indexlib::index_base::Version v1;
    v1.SetVersionId(1);
    EXPECT_CALL(*reader, GetVersion()).WillOnce(Return(v1));
    IndexPartitionReader::AccessCounterMap counters;
    EXPECT_CALL(*reader, GetIndexAccessCounters()).WillRepeatedly(ReturnRef(counters));
    EXPECT_CALL(*reader, GetAttributeAccessCounters()).WillRepeatedly(ReturnRef(counters));
    ret2 = p.load(target, false);

    auto ipa = dynamic_cast<IndexPartitionAdapter *>(p._indexlibAdapter.get());
    ASSERT_TRUE(ipa != nullptr);
    ASSERT_TRUE(ipa->_indexPartition.get() != nullptr);
    ASSERT_EQ(TS_LOADED, ret2.status);
    ASSERT_TRUE(_current->getFullIndexLoaded());
    ASSERT_FALSE(_current->getLoadedIndexRoot().empty());
    ASSERT_FALSE(_current->getLoadedConfigPath().empty());
    ASSERT_EQ(1, _current->getIncVersion());
    data = p.getPartitionData();
    ASSERT_TRUE(data);
    ASSERT_TRUE(p.isInUse());

    // load inc
    target.setIncVersion(2);
    _current->setDeployStatus(2, DS_DEPLOYDONE); // mark deploy done

    // reopen failed
    EXPECT_CALL(*indexPart, ReOpen(false, 2)).WillOnce(Return(IndexPartition::OS_LACK_OF_MEMORY));
    ret2 = p.load(target, false);
    ASSERT_EQ(TS_ERROR_LACK_MEM, ret2.status);
    ASSERT_EQ(ERROR_NONE, ret2.error);
    ASSERT_EQ(1, _current->getIncVersion());

    // reopen succ
    EXPECT_CALL(*indexPart, ReOpen(true, 2)).WillOnce(Return(IndexPartition::OS_OK));

    indexlib::index_base::Version v2;
    v2.SetVersionId(2);
    EXPECT_CALL(*reader, GetVersion()).WillOnce(Return(v2));

    ret2 = p.load(target, true);
    ASSERT_EQ(TS_LOADED, ret2.status);
    ASSERT_EQ(ERROR_NONE, ret2.error);
    ASSERT_EQ(2, _current->getIncVersion());

    // unload
    data.reset();
    p.unload();
    ASSERT_FALSE(p._indexlibAdapter);
    checkUnloadedStatus();
}

TEST_F(SuezIndexPartitionTest, testForceLoad) {
    // bugfix for: https://aone.alibaba-inc.com/v2/project/1055745/bug/39609840
    NiceMockSuezIndexPartition p(_resource, _current);

    auto indexPart = MockIndexPartition::MakeNice();
    IndexPartitionPtr partPtr(indexPart);
    auto indexPartAdapter = std::make_unique<IndexPartitionAdapter>("", "", partPtr);

    p._indexlibAdapter = std::move(indexPartAdapter);
    DummyTableBuilder *builder1 = new DummyTableBuilder();
    p._tableBuilder.reset(builder1);
    EXPECT_CALL(*indexPart, ReOpen(true, _)).WillOnce(Return(IndexPartition::OS_OK));
    EXPECT_CALL(p, createTableBuilder(_, _)).WillOnce(Return(new DummyTableBuilder()));
    ASSERT_EQ(TS_LOADED, p.loadInc(PartitionProperties(_resource.pid), 0, true));
    ASSERT_TRUE(p._tableBuilder.get() != nullptr);
    ASSERT_TRUE(p._tableBuilder.get() != builder1);
    p._indexlibAdapter.reset();
}

ACTION_P2(doLoadTableConfig, enableDirectWrite, enableDrc) {
    arg1._realtime = true;
    arg1._directWrite = true;
    arg1._drcConfig._enabled = true;
}

TEST_F(SuezIndexPartitionTest, testDirectWriteMode) {
    auto legacySchema =
        makeSchema("id:int64;buyer_age:uint32;attr_str:string;", "id:primarykey64:id", "buyer_age;attr_str");
    auto jsonStr = FastToJsonString(*legacySchema);
    std::shared_ptr<indexlibv2::config::TabletSchema> schema =
        indexlibv2::framework::TabletSchemaLoader::LoadSchema(jsonStr);
    ASSERT_TRUE(schema);

    auto target = TableMetaUtil::makeTarget(1, "t1_index/0/", "/t1_config/0");
    target.setRoleType(RT_LEADER);
    NiceMockSuezIndexPartition p(_resource, _current);

    EXPECT_CALL(p, doDeployConfig("/t1_config/0", _)).WillOnce(Return(DS_DEPLOYDONE));
    EXPECT_CALL(p, doDeployIndex(_, _)).WillOnce(Return(DS_DEPLOYDONE));

    auto ret = p.deploy(target, false);
    ASSERT_EQ(DS_DEPLOYDONE, ret.status);
    _current->setDeployStatus(1, DS_DEPLOYDONE); // TODO: move update status to partition

    EXPECT_CALL(p, loadTableConfig(_, _)).WillRepeatedly(DoAll(doLoadTableConfig(true, true), Return(true)));

    // load full
    auto mockTablet = std::make_shared<indexlibv2::framework::MockTablet>();
    auto indexlibAdapter = std::make_unique<TabletAdapter>(mockTablet, "", "", schema, RT_LEADER);
    EXPECT_CALL(p, createTableWriter(_)).WillOnce(Return(std::make_shared<TableWriter>()));
    EXPECT_CALL(p, createIndexlibAdapter(_, _)).WillOnce(Return(ByMove(std::move(indexlibAdapter))));
    indexlibv2::framework::VersionCoord fullVersionCoord(1);
    EXPECT_CALL(*mockTablet, Open(_, _, _, fullVersionCoord)).WillOnce(Return(indexlib::Status::OK()));
    indexlibv2::framework::TabletInfos tabletInfos;
    tabletInfos.SetLoadedPublishVersion(indexlibv2::framework::Version(1));
    EXPECT_CALL(*mockTablet, GetTabletSchema()).WillRepeatedly(Return(schema));
    EXPECT_CALL(*mockTablet, GetTabletInfos()).WillRepeatedly(Return(&tabletInfos));
    auto mockBuilder = new NiceMockTableBuilder();
    EXPECT_CALL(p, createTableBuilder(_, _)).WillOnce(Return(mockBuilder));
    EXPECT_CALL(*mockBuilder, start()).WillOnce(Return(true));
    EXPECT_CALL(*mockBuilder, isRecovered()).WillOnce(Return(true));
    EXPECT_CALL(p, initLogReplicator(_, _)).WillOnce(Return(true));
    auto ret2 = p.load(target, false);
    ASSERT_EQ(TS_LOADED, ret2.status);
    ASSERT_EQ(ERROR_NONE, ret2.error);
    ASSERT_TRUE(p._indexlibAdapter);
    ASSERT_TRUE(p._tableBuilder);
    ASSERT_TRUE(p._tableWriter);
    ASSERT_TRUE(p._tableWriter->getIndex());

    // force load inc
    target.setIncVersion(2);
    _current->setDeployStatus(2, DS_DEPLOYDONE); // mark deploy done
    EXPECT_CALL(p, closeLogReplicator());
    EXPECT_CALL(*mockBuilder, stop());
    indexlibv2::framework::VersionCoord versionCoord(2);
    EXPECT_CALL(*mockTablet, Reopen(_, versionCoord)).WillOnce(Return(indexlib::Status::OK()));
    auto mockBuilder2 = new NiceMockTableBuilder();
    EXPECT_CALL(p, createTableBuilder(_, _)).WillOnce(Return(mockBuilder2));
    EXPECT_CALL(*mockBuilder2, start()).WillOnce(Return(true));
    EXPECT_CALL(*mockBuilder2, isRecovered()).WillOnce(Return(true));
    EXPECT_CALL(p, initLogReplicator(_, _)).WillOnce(Return(true));
    ret2 = p.load(target, true);
    ASSERT_EQ(TS_LOADED, ret2.status);
    ASSERT_EQ(ERROR_NONE, ret2.error);
    ASSERT_TRUE(p._indexlibAdapter);
    ASSERT_TRUE(p._tableBuilder);
    ASSERT_TRUE(p._tableWriter);
    ASSERT_TRUE(p._tableWriter->getIndex());

    // unload
    EXPECT_CALL(p, closeLogReplicator());
    EXPECT_CALL(*mockBuilder2, stop());
    EXPECT_CALL(*mockTablet, Close());
    p.unload();
    ASSERT_FALSE(p._indexlibAdapter);
    ASSERT_FALSE(p._tableBuilder);
    ASSERT_FALSE(p._tableWriter);
}

} // namespace suez
