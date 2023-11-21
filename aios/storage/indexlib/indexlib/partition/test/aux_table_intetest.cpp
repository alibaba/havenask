#include "autil/Thread.h"
#include "indexlib/common_define.h"
#include "indexlib/config/test/schema_maker.h"
#include "indexlib/config/virtual_attribute_config_creator.h"
#include "indexlib/index/normal/inverted_index/accessor/multi_field_index_reader.h"
#include "indexlib/index_base/index_meta/version_loader.h"
#include "indexlib/partition/index_application.h"
#include "indexlib/partition/join_cache/join_cache_initializer_creator.h"
#include "indexlib/partition/join_cache/join_docid_reader.h"
#include "indexlib/partition/join_cache/join_docid_reader_creator.h"
#include "indexlib/partition/join_cache/join_info.h"
#include "indexlib/partition/online_partition.h"
#include "indexlib/partition/partition_reader_snapshot.h"
#include "indexlib/partition/table_reader_container.h"
#include "indexlib/partition/table_reader_container_updater.h"
#include "indexlib/partition/test/fake_memory_quota_controller.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/single_field_partition_data_provider.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/test/build_test_util.h"

using namespace std;
using namespace autil;

using namespace indexlib::test;
using namespace indexlib::index;
using namespace indexlib::partition;
using namespace indexlib::config;
using namespace indexlib::file_system;
using namespace indexlib::index_base;

namespace indexlib { namespace partition {
class AuxTableInteTest : public INDEXLIB_TESTBASE_WITH_PARAM<int>
{
public:
    AuxTableInteTest();
    ~AuxTableInteTest();

    DECLARE_CLASS_NAME(AuxTableInteTest);

private:
    struct VirtualAttributeInfo {
        VirtualAttributeInfo(const std::string& _mainTableName, const std::string& _auxTableName,
                             const std::string& _virtualAttributeName, bool _isSub)
            : mainTableName(_mainTableName)
            , auxTableName(_auxTableName)
            , virtualAttributeName(_virtualAttributeName)
            , isSub(_isSub)
        {
        }
        std::string mainTableName;
        std::string auxTableName;
        std::string virtualAttributeName;
        bool isSub;
    };

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestAuxMemoryQuota();
    void TestMultiJoinPartition_1();
    void TestMultiJoinPartition_2();
    void TestMultiApplication();
    void TestAddVirtualAttributeFail();
    void TestSameVirtualAttribute();
    void TestUnloadAuxTable();

private:
    void RemoveVirtualAttributeInfo(const std::string& mainTableName, const std::string& auxTableName,
                                    std::vector<VirtualAttributeInfo>& virtualAttrInfos);
    void InnerTestAuxMemoryQuota(util::MemoryQuotaControllerPtr fakeController = util::MemoryQuotaControllerPtr());
    void InnerTestJoinIndexPartition(const partition::JoinRelationMap& joinRelations);
    bool CheckCleanExpiredVirtualAttribute(const partition::IndexApplicationPtr& app,
                                           const std::vector<VirtualAttributeInfo>& expiredVirtualAttributes);
    void AssertQuery(const partition::IndexApplicationPtr& app, const std::vector<int>& queryDocids,
                     const std::vector<int>& expectedDocids,
                     std::vector<VirtualAttributeInfo>& expiredVirtualAttributes, std::string auxTable = "aux",
                     std::string mainTable = "main");
    void AssertPkQuery(const partition::IndexApplicationPtr& app, const std::vector<int>& queryPks,
                       const std::vector<int>& expectedDocids, std::string mainTable);
    void AssertQuery(const partition::IndexApplicationPtr& app, const std::vector<int>& queryDocids,
                     const std::vector<int>& expectedDocids, std::string auxTable = "aux",
                     std::string mainTable = "main")
    {
        std::vector<VirtualAttributeInfo> expiredVirtualAttributes;
        AssertQuery(app, queryDocids, expectedDocids, expiredVirtualAttributes, auxTable, mainTable);
    }

    void QueryFullData(partition::IndexApplicationPtr app, const partition::JoinRelationMap& joinRelations,
                       const std::vector<int>& queryDocids, const std::vector<int>& expectedDocids,
                       std::vector<VirtualAttributeInfo>& expiredVirtualAttributes);

    std::string GetJoinVirtualAttrName(const partition::IndexApplicationPtr& app, std::string auxTable = "aux",
                                       std::string mainTable = "main");
    config::IndexPartitionSchemaPtr CreateMainSchema(std::string tableName, bool hasSub = false);
    config::IndexPartitionSchemaPtr CreateJoinSchema(std::string joinTableName);
    std::string MakeAddCmdString(const std::vector<int>& pks, const std::vector<int>& attrs, uint64_t ts);
    std::string MakeMainDocs(const std::vector<int>& pks, uint64_t ts, bool hasSub = true);
    void BuildMainTable(test::PartitionStateMachinePtr psm);
    void BuildAuxTable(test::PartitionStateMachinePtr psm);
    void UpdateMainFull(std::vector<test::PartitionStateMachinePtr>& mainPsms,
                        std::vector<partition::IndexPartitionPtr>& partVec, std::map<std::string, int>& tableName2Part,
                        std::string mainTableName, std::string fullDocs);
    void UpdateAuxFull(std::vector<test::PartitionStateMachinePtr>& auxPsm,
                       std::vector<partition::IndexPartitionPtr>& partVec, std::map<std::string, int>& tableName2Part,
                       std::string joinTableName, std::string fullDocs);
    void ExecuteBuild(const std::vector<test::PartitionStateMachinePtr>& mainPsms,
                      const std::vector<test::PartitionStateMachinePtr>& auxPsms);

private:
    std::string mMainRoot;
    std::string mAuxRoot;
    config::IndexPartitionSchemaPtr mMainSchema;
    config::IndexPartitionSchemaPtr mAuxSchema;
    IE_LOG_DECLARE();
};

INSTANTIATE_TEST_CASE_P(BuildMode, AuxTableInteTest, testing::Values(0, 1, 2));

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(AuxTableInteTest, TestAuxMemoryQuota);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(AuxTableInteTest, TestMultiJoinPartition_1);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(AuxTableInteTest, TestMultiJoinPartition_2);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(AuxTableInteTest, TestMultiApplication);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(AuxTableInteTest, TestAddVirtualAttributeFail);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(AuxTableInteTest, TestSameVirtualAttribute);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(AuxTableInteTest, TestUnloadAuxTable);

IE_LOG_SETUP(partition, AuxTableInteTest);

AuxTableInteTest::AuxTableInteTest() {}

AuxTableInteTest::~AuxTableInteTest() {}

void AuxTableInteTest::CaseSetUp()
{
    mMainRoot = GET_TEMP_DATA_PATH() + "main_part";
    mAuxRoot = GET_TEMP_DATA_PATH() + "aux_part";
    string field = "pk:int64;"
                   "field:int64";
    string index = "pk:primarykey64:pk";
    string attribute = "field";
    mMainSchema = SchemaMaker::MakeSchema(field, index, attribute, "");
    mMainSchema->SetSchemaName("main");
    mMainSchema->SetAutoUpdatePreference(false);
    mAuxSchema = SchemaMaker::MakeSchema(field, index, attribute, "");
    mAuxSchema->SetSchemaName("aux");
    mAuxSchema->SetAutoUpdatePreference(false);
}

void AuxTableInteTest::CaseTearDown() {}

string AuxTableInteTest::GetJoinVirtualAttrName(const partition::IndexApplicationPtr& app, std::string auxTable,
                                                std::string mainTable)
{
    PartitionReaderSnapshotPtr snapshot = app->CreateSnapshot();
    JoinInfo joinInfo;
    snapshot->GetAttributeJoinInfo(auxTable, mainTable, joinInfo);
    return joinInfo.GetJoinVirtualAttrName();
}

IndexPartitionPtr GetIndexPartition(const IndexApplicationPtr& app, const std::string& partName)
{
    uint32_t idx = app->mTableName2PartitionIdMap[partName];
    return app->mIndexPartitionVec[idx];
}

bool AuxTableInteTest::CheckCleanExpiredVirtualAttribute(
    const IndexApplicationPtr& app, const std::vector<VirtualAttributeInfo>& expiredVirtualAttributes)
{
    PartitionReaderSnapshotPtr snapshot = app->CreateSnapshot();
    for (size_t i = 0; i < expiredVirtualAttributes.size(); i++) {
        bool isSubJoin = expiredVirtualAttributes[i].isSub;
        string oldVirtualAttr = expiredVirtualAttributes[i].virtualAttributeName;
        IndexPartitionPtr mainPart = GetIndexPartition(app, expiredVirtualAttributes[i].mainTableName);
        // make sure clean resource to clean old virtual attribute
        while (true) {
            OnlinePartition* onlinePart = (OnlinePartition*)(mainPart.get());
            if (onlinePart->ExecuteTask(OnlinePartitionTaskItem::TT_CLEAN_RESOURCE)) {
                break;
            }
        }
        IndexPartitionSchemaPtr mainSchema = mainPart->GetSchema();
        AttributeSchemaPtr attrSchema = isSubJoin
                                            ? mainSchema->GetSubIndexPartitionSchema()->GetVirtualAttributeSchema()
                                            : mainSchema->GetVirtualAttributeSchema();
        if (attrSchema->GetAttributeConfig(oldVirtualAttr)) {
            std::cout << "failed here" << std::endl;
            return false;
        }
        // make sure clean resource to clean old index
        ((OnlinePartition*)mainPart.get())->ExecuteTask(OnlinePartitionTaskItem::TT_CLEAN_RESOURCE);
        auto directory = mainPart->GetRootDirectory();
        auto partData = isSubJoin ? mainPart->GetPartitionData()->GetSubPartitionData() : mainPart->GetPartitionData();
        for (PartitionData::Iterator ite = partData->Begin(); ite != partData->End(); ite++) {
            if (ite->GetSegmentInfo()->docCount > 0) {
                if (ite->GetAttributeDirectory(oldVirtualAttr, false)) {
                    std::cout << "old virtual attr:" << oldVirtualAttr << std::endl;
                    std::cout << "failed here1" << std::endl;
                    return false;
                }
            }
        }
    }
    return true;
}

void AuxTableInteTest::TestAuxMemoryQuota()
{
    InnerTestAuxMemoryQuota();
    for (size_t i = 0; i < 10; i++) {
        FakeMemoryQuotaController* fakeController = new FakeMemoryQuotaController(1024 * 1024 * 1024, i, 0);
        util::MemoryQuotaControllerPtr controller(fakeController);
        InnerTestAuxMemoryQuota(controller);
    }
}

void AuxTableInteTest::InnerTestAuxMemoryQuota(util::MemoryQuotaControllerPtr fakeController)
{
    tearDown();
    setUp();
    IndexPartitionResource partitionResource;
    if (fakeController) {
        partitionResource.memoryQuotaController = fakeController;
    }
    PartitionStateMachine mainPsm(false, partitionResource);
    PartitionStateMachine auxPsm(false, partitionResource);
    IndexPartitionOptions options = util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());

    mainPsm.Init(mMainSchema, options, mMainRoot);
    auxPsm.Init(mAuxSchema, options, mAuxRoot);

    string fullDocs = MakeAddCmdString({1, 2, 3}, {1, 2, 3}, 1);
    if (!mainPsm.Transfer(BUILD_FULL, fullDocs, "", ""))
        return;
    fullDocs = MakeAddCmdString({3, 2, 1}, {1, 2, 3}, 1);
    if (!auxPsm.Transfer(BUILD_FULL, fullDocs, "", ""))
        return;

    vector<IndexPartitionPtr> partVec;
    partVec.push_back(mainPsm.GetIndexPartition());
    partVec.push_back(auxPsm.GetIndexPartition());
    IndexApplicationPtr app(new IndexApplication);
    JoinRelationMap joinRelations;
    joinRelations["main"] = {JoinField("field", "aux", true, true)};

    while (!app->Init(partVec, joinRelations) && fakeController) {
        app.reset(new IndexApplication);
    }

    AssertQuery(app, {0, 1, 2}, {2, 1, 0});

    string incDocs1 = MakeAddCmdString({3, 2, 1, 4, 5, 6}, {3, 2, 1, 4, 5, 6}, 2);
    ASSERT_TRUE(mainPsm.Transfer(BUILD_INC_NO_REOPEN, incDocs1, "", ""));
    while (!mainPsm.Transfer(PE_REOPEN, "", "", "") && fakeController)
        ;
    AssertQuery(app, {0, 1, 2}, {0, 1, 2});
    string incDocs2 = MakeAddCmdString({4, 6, 5}, {3, 2, 1}, 2);
    ASSERT_TRUE(auxPsm.Transfer(BUILD_INC_NO_REOPEN, incDocs2, "", ""));
    while (!auxPsm.Transfer(PE_REOPEN, "", "", "") && fakeController)
        ;
    AssertQuery(app, {3, 4, 5}, {3, 5, 4});
}

void AuxTableInteTest::TestMultiJoinPartition_1()
{
    JoinRelationMap joinRelations;
    joinRelations["mainTableA"] = {JoinField("field0", "auxTableA", true, true)};
    joinRelations["mainTableB"] = {JoinField("field0", "auxTableA", true, true),
                                   JoinField("field1", "auxTableB", true, true)};

    InnerTestJoinIndexPartition(joinRelations);
}

void AuxTableInteTest::TestMultiJoinPartition_2()
{
    // test sub field
    JoinRelationMap joinRelations;
    joinRelations["mainTableA"] = {JoinField("sub_field0", "auxTableA", true, true)};
    joinRelations["mainTableB"] = {JoinField("sub_field0", "auxTableA", true, true),
                                   JoinField("field1", "auxTableB", true, true)};

    InnerTestJoinIndexPartition(joinRelations);
}

void AuxTableInteTest::TestAddVirtualAttributeFail()
{
    map<string, int> tableName2Part;
    vector<PartitionStateMachinePtr> mainPsms;
    vector<PartitionStateMachinePtr> auxPsms;
    vector<IndexPartitionPtr> partVec;
    JoinRelationMap joinRelations;
    joinRelations["mainTableA"] = {JoinField("field0", "auxTableA", true, true)};

    // prepare main tables, aux tables
    string mainTableName = "mainTableA";
    string fullDocs = MakeMainDocs({1, 2, 3}, 1);
    UpdateMainFull(mainPsms, partVec, tableName2Part, mainTableName, fullDocs);
    const string& joinTableName = "auxTableA";
    fullDocs = MakeAddCmdString({3, 2, 1}, {3, 2, 1}, 1);
    UpdateAuxFull(auxPsms, partVec, tableName2Part, joinTableName, fullDocs);
    IndexApplicationPtr app(new IndexApplication);
    ASSERT_TRUE(app->Init(partVec, joinRelations));
    string rtDocs1 = MakeMainDocs({4, 5, 6}, 2);
    ASSERT_TRUE(mainPsms.back()->Transfer(BUILD_RT, rtDocs1, "", ""));
    mainPsms.back()->mPartitionResource.memoryQuotaController->_localFreeQuota = -1;
    app.reset();
    string rtDocs2 = MakeMainDocs({7, 8, 9}, 3);
    ASSERT_TRUE(mainPsms.back()->Transfer(BUILD_RT_SEGMENT, rtDocs2, "", ""));
}

void AuxTableInteTest::RemoveVirtualAttributeInfo(const string& mainTableName, const string& auxTableName,
                                                  vector<VirtualAttributeInfo>& virtualAttrInfos)
{
    for (size_t i = 0; i < virtualAttrInfos.size(); i++) {
        if (virtualAttrInfos[i].mainTableName == mainTableName && virtualAttrInfos[i].auxTableName == auxTableName) {
            virtualAttrInfos.erase(virtualAttrInfos.begin() + i);
            break;
        }
    }
}

void AuxTableInteTest::InnerTestJoinIndexPartition(const JoinRelationMap& joinRelations)
{
    vector<PartitionStateMachinePtr> mainPsms;
    vector<PartitionStateMachinePtr> auxPsms;
    vector<IndexPartitionPtr> partVec;
    vector<VirtualAttributeInfo> virtualAttrInfos;
    map<string, int> tableName2Part;
    // prepare main tables, aux tables
    for (auto iter : joinRelations) {
        string mainTableName = iter.first;
        string fullDocs = MakeMainDocs({1, 2, 3}, 1);
        UpdateMainFull(mainPsms, partVec, tableName2Part, mainTableName, fullDocs);
        for (auto joinField : iter.second) {
            const string& joinTableName = joinField.joinTableName;
            if (tableName2Part.find(joinTableName) != tableName2Part.end())
                continue;
            fullDocs = MakeAddCmdString({3, 2, 1}, {3, 2, 1}, 1);
            UpdateAuxFull(auxPsms, partVec, tableName2Part, joinTableName, fullDocs);
        }
    }

    // first: test join result right
    IndexApplicationPtr app(new IndexApplication);
    ASSERT_TRUE(app->Init(partVec, joinRelations));
    IndexApplicationPtr appDup(new IndexApplication);
    ASSERT_TRUE(appDup->Init(partVec, joinRelations));
    QueryFullData(app, joinRelations, {0, 1, 2}, {2, 1, 0}, virtualAttrInfos);
    QueryFullData(appDup, joinRelations, {0, 1, 2}, {2, 1, 0}, virtualAttrInfos);
    // second: test multithread rt build && inc build not core, join relation right
    ExecuteBuild(mainPsms, auxPsms);
    // sleep(3);
    ASSERT_TRUE(CheckCleanExpiredVirtualAttribute(app, virtualAttrInfos));
    QueryFullData(app, joinRelations, {3, 4, 5, 6, 7}, {19, 18, 17, 16, 15}, virtualAttrInfos);
    QueryFullData(appDup, joinRelations, {3, 4, 5, 6, 7}, {19, 18, 17, 16, 15}, virtualAttrInfos);

    // third: test aux table A change
    IndexApplicationPtr app2(new IndexApplication);
    string fullDocs = MakeAddCmdString({6, 5, 4, 3, 2, 1}, {6, 5, 4, 3, 2, 1}, 1);
    UpdateAuxFull(auxPsms, partVec, tableName2Part, "auxTableA", fullDocs);
    ASSERT_TRUE(app2->Init(partVec, joinRelations));
    AssertQuery(app2, {0, 1, 2}, {5, 4, 3}, "auxTableA", "mainTableA");
    AssertQuery(app2, {0, 1, 2}, {5, 4, 3}, "auxTableA", "mainTableB");
    string incDocs = MakeAddCmdString({9, 8, 7}, {3, 2, 1}, 2);
    INDEXLIB_TEST_TRUE(auxPsms.back()->Transfer(BUILD_INC, incDocs, "", ""));
    AssertQuery(app2, {6, 7, 8}, {8, 7, 6}, "auxTableA", "mainTableA");
    AssertQuery(app2, {6, 7, 8}, {8, 7, 6}, "auxTableA", "mainTableB");
    AssertQuery(app2, {3, 4, 5, 6, 7}, {19, 18, 17, 16, 15}, "auxTableB", "mainTableB");
    // check old app right
    QueryFullData(app, joinRelations, {3, 4, 5, 6, 7}, {19, 18, 17, 16, 15}, virtualAttrInfos);
    QueryFullData(appDup, joinRelations, {3, 4, 5, 6, 7}, {19, 18, 17, 16, 15}, virtualAttrInfos);

    // four: old app reset, check old virtual attribute not exist, check app2 right
    app.reset();
    // sleep(3);
    // aux table B not change
    RemoveVirtualAttributeInfo("mainTableB", "auxTableB", virtualAttrInfos);
    ASSERT_FALSE(CheckCleanExpiredVirtualAttribute(app2, virtualAttrInfos));

    appDup.reset();
    IE_LOG(ERROR, "xxxxxxxxxxxxxxxxxxxxxxx");
    sleep(1);
    RemoveVirtualAttributeInfo("mainTableB", "auxTableB", virtualAttrInfos);
    ASSERT_TRUE(CheckCleanExpiredVirtualAttribute(app2, virtualAttrInfos));
    IE_LOG(ERROR, "xxxxxxxxxxxxxxxxxxxxxxx");

    AssertQuery(app2, {6, 7, 8}, {8, 7, 6}, "auxTableA", "mainTableA");
    AssertQuery(app2, {6, 7, 8}, {8, 7, 6}, "auxTableA", "mainTableB");
    AssertQuery(app2, {3, 4, 5, 6, 7}, {19, 18, 17, 16, 15}, "auxTableB", "mainTableB");

    // five: test main table change, app2 result right, app3 result right
    IndexApplicationPtr app3(new IndexApplication);
    fullDocs = MakeMainDocs({3, 4, 5}, 1);
    UpdateMainFull(mainPsms, partVec, tableName2Part, "mainTableA", fullDocs);
    ASSERT_TRUE(app3->Init(partVec, joinRelations));
    AssertQuery(app3, {0, 1, 2}, {3, 2, 1}, "auxTableA", "mainTableA");
    incDocs = MakeMainDocs({6, 7, 8}, 2);
    INDEXLIB_TEST_TRUE(mainPsms.back()->Transfer(BUILD_INC, incDocs, "", ""));
    AssertQuery(app3, {3, 4, 5}, {0, 8, 7}, "auxTableA", "mainTableA");
    AssertQuery(app2, {6, 7, 8}, {8, 7, 6}, "auxTableA", "mainTableA");
    app2.reset();
    app3.reset();
}

void AuxTableInteTest::QueryFullData(IndexApplicationPtr app, const JoinRelationMap& joinRelations,
                                     const std::vector<int>& queryDocids, const std::vector<int>& expectedDocids,
                                     vector<VirtualAttributeInfo>& virtualAttrInfos)
{
    virtualAttrInfos.clear();
    for (auto iter : joinRelations) {
        string mainTableName = iter.first;
        for (auto joinField : iter.second) {
            const string& joinTableName = joinField.joinTableName;
            AssertQuery(app, queryDocids, expectedDocids, virtualAttrInfos, joinTableName, mainTableName);
        }
    }
}

void AuxTableInteTest::AssertPkQuery(const IndexApplicationPtr& app, const vector<int>& queryPks,
                                     const vector<int>& expectedDocids, string mainTable)
{
    autil::mem_pool::Pool pool;

    PartitionReaderSnapshotPtr snapshot = app->CreateSnapshot();

    index::legacy::MultiFieldIndexReaderPtr reader =
        DYNAMIC_POINTER_CAST(index::legacy::MultiFieldIndexReader, snapshot->GetInvertedIndexReader(mainTable, "pk"));
    assert(reader);
    InvertedIndexReaderPtr singleIndexReader = reader->GetInvertedIndexReader("pk");
    PrimaryKeyIndexReaderPtr pkReader = DYNAMIC_POINTER_CAST(PrimaryKeyIndexReader, singleIndexReader);
    assert(pkReader);
    for (size_t i = 0; i < queryPks.size(); i++) {
        ASSERT_EQ((docid_t)expectedDocids[i], pkReader->Lookup(StringUtil::toString(queryPks[i])));
    }
}

void AuxTableInteTest::AssertQuery(const IndexApplicationPtr& app, const vector<docid_t>& queryDocids,
                                   const vector<docid_t>& expectedDocids,
                                   std::vector<VirtualAttributeInfo>& virtualAttributeInfos, string auxTable,
                                   string mainTable)
{
    autil::mem_pool::Pool pool;

    PartitionReaderSnapshotPtr snapshot = app->CreateSnapshot();
    JoinInfo joinInfo;
    ASSERT_TRUE(snapshot->GetAttributeJoinInfo(auxTable, mainTable, joinInfo));
    VirtualAttributeInfo attrInfo(mainTable, auxTable, joinInfo.GetJoinVirtualAttrName(), joinInfo.IsSubJoin());
    virtualAttributeInfos.push_back(attrInfo);

    JoinDocidReaderBase* joinReader = JoinDocidReaderCreator::Create(joinInfo, &pool);
    assert(joinReader);
    ASSERT_EQ(expectedDocids.size(), queryDocids.size());
    for (size_t i = 0; i < queryDocids.size(); i++) {
        ASSERT_EQ(expectedDocids[i], joinReader->GetAuxDocid(queryDocids[i]));
    }
    POOL_DELETE_CLASS(joinReader);
}

std::string AuxTableInteTest::MakeAddCmdString(const vector<int>& pks, const vector<int>& attrs, uint64_t ts)
{
    std::string result;
    for (size_t i = 0; i < pks.size(); i++) {
        result += "cmd=add,pk=" + StringUtil::toString(pks[i]) + ",field=" + StringUtil::toString(attrs[i]) +
                  ",ts=" + StringUtil::toString(ts) + ";";
    }
    return result;
}

IndexPartitionSchemaPtr AuxTableInteTest::CreateMainSchema(string tableName, bool hasSub)
{
    string field = "pk:int64;";
    string attribute;
    for (size_t i = 0; i <= 2; i++) {
        field += "field" + StringUtil::toString(i) + ":int64";
        attribute += "field" + StringUtil::toString(i);
        field += ";";
        attribute += ";";
    }
    string index = "pk:primarykey64:pk";
    IndexPartitionSchemaPtr mainSchema = SchemaMaker::MakeSchema(field, index, attribute, "");
    mainSchema->SetSchemaName(tableName);
    if (hasSub) {
        IndexPartitionSchemaPtr subSchema = SchemaMaker::MakeSchema(
            "sub_field0:int64;sub_field1:string", "sub_pk:primarykey64:sub_field0;sub_field1:string:sub_field1;",
            "sub_field0;sub_field1", "");
        subSchema->SetSchemaName(tableName);
        mainSchema->SetSubIndexPartitionSchema(subSchema);
    }
    return mainSchema;
}

IndexPartitionSchemaPtr AuxTableInteTest::CreateJoinSchema(string joinTableName)
{
    string field = "pk:int64;"
                   "field:int64";
    string index = "pk:primarykey64:pk";
    string attribute = "field";
    IndexPartitionSchemaPtr auxSchema = SchemaMaker::MakeSchema(field, index, attribute, "");
    auxSchema->SetSchemaName(joinTableName);
    return auxSchema;
}

string AuxTableInteTest::MakeMainDocs(const vector<int>& pks, uint64_t timestamp, bool hasSub)
{
    string result;
    string ts = StringUtil::toString(timestamp);
    for (size_t i = 0; i < pks.size(); i++) {
        string pk = StringUtil::toString(pks[i]);
        result += "cmd=add,pk=" + pk;
        for (size_t j = 0; j <= 2; j++) {
            result += ",field" + StringUtil::toString(j) + "=" + pk;
        }
        result += ",ts=" + ts;
        if (hasSub) {
            result += ",sub_field0=" + pk + ",sub_filed1=str1";
        }
        result += ";";
    }
    return result;
}

void AuxTableInteTest::BuildMainTable(PartitionStateMachinePtr psm)
{
    int startDocs = 4;
    uint64_t ts = 5;
    int segmentDocCount = 20;
    for (int j = 0; j < 3; j++) {
        int incDocs = startDocs;
        vector<int> pks;
        for (int i = startDocs; i < startDocs + segmentDocCount; i++) {
            pks.push_back(i);
        }

        string rtDocs = MakeMainDocs(pks, ts++);
        startDocs += segmentDocCount;
        ASSERT_TRUE(psm->Transfer(BUILD_RT_SEGMENT, rtDocs, "", ""));
        pks.clear();
        for (int i = startDocs; i < startDocs + segmentDocCount; i++) {
            pks.push_back(i);
        }

        rtDocs = MakeMainDocs(pks, ts++);
        startDocs += segmentDocCount;
        ASSERT_TRUE(psm->Transfer(BUILD_RT_SEGMENT, rtDocs, "", ""));
        pks.clear();
        for (int k = incDocs; k < startDocs; k++) {
            pks.push_back(k);
        }
        ASSERT_TRUE(psm->Transfer(BUILD_INC, MakeMainDocs(pks, ts++), "", ""));
    }
}

void AuxTableInteTest::BuildAuxTable(PartitionStateMachinePtr psm)
{
    size_t pkfrom = 4, pkto = 20;
    uint64_t ts = 5;
    for (size_t startDocs = pkto; startDocs >= pkfrom;) {
        int incDocs = startDocs;
        for (int j = 0; j < 2; j++) {
            vector<int> pks;
            for (int j = 0; j < 5 && startDocs >= pkfrom; j++) {
                pks.push_back(startDocs);
                startDocs--;
            }
            if (pks.empty())
                break;
            string rtDocs = MakeAddCmdString(pks, pks, ts++);
            ASSERT_TRUE(psm->Transfer(BUILD_RT_SEGMENT, rtDocs, "", ""));
        }
        vector<int> pks;
        for (size_t j = incDocs; j > startDocs; j--) {
            pks.push_back(j);
        }
        ASSERT_TRUE(psm->Transfer(BUILD_INC, MakeAddCmdString(pks, pks, ts++), "", ""));
    }
}

void AuxTableInteTest::ExecuteBuild(const vector<PartitionStateMachinePtr>& mainPsms,
                                    const vector<PartitionStateMachinePtr>& auxPsms)
{
    std::vector<autil::ThreadPtr> threads;
    for (size_t i = 0; i < mainPsms.size(); i++) {
        auto psm = mainPsms[i];
        threads.push_back(autil::Thread::createThread(bind(&AuxTableInteTest::BuildMainTable, this, psm)));
    }
    for (size_t i = 0; i < auxPsms.size(); i++) {
        auto psm = auxPsms[i];
        threads.push_back(autil::Thread::createThread(bind(&AuxTableInteTest::BuildAuxTable, this, psm)));
    }
    for (size_t i = 0; i < threads.size(); i++) {
        threads[i]->join();
    }
}

void AuxTableInteTest::UpdateMainFull(vector<PartitionStateMachinePtr>& mainPsms, vector<IndexPartitionPtr>& partVec,
                                      map<string, int>& tableName2Part, string mainTableName, string fullDocs)
{
    static int generation = 0;
    static IndexPartitionOptions options =
        util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
    generation++;
    string generationStr = StringUtil::toString(generation);
    PartitionStateMachinePtr mainPsm(new PartitionStateMachine());
    string indexPath = mMainRoot + "/" + generationStr + "/" + mainTableName;
    mainPsms.push_back(mainPsm);
    ASSERT_TRUE(mainPsm->Init(CreateMainSchema(mainTableName, true), options, indexPath, generationStr));
    INDEXLIB_TEST_TRUE(mainPsm->Transfer(BUILD_FULL, fullDocs, "", ""));
    if (tableName2Part.find(mainTableName) == tableName2Part.end()) {
        tableName2Part[mainTableName] = partVec.size();
        partVec.push_back(mainPsm->GetIndexPartition());
    } else {
        int idx = tableName2Part[mainTableName];
        partVec[idx] = mainPsm->GetIndexPartition();
    }
}

void AuxTableInteTest::UpdateAuxFull(vector<PartitionStateMachinePtr>& auxPsms, vector<IndexPartitionPtr>& partVec,
                                     map<string, int>& tableName2Part, string joinTableName, string fullDocs)
{
    static int generation = 0;
    static IndexPartitionOptions options =
        util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
    generation++;
    string generationStr = StringUtil::toString(generation);
    string indexPath = mAuxRoot + "/" + generationStr + "/" + joinTableName;
    PartitionStateMachinePtr auxPsm(new PartitionStateMachine);
    auxPsms.push_back(auxPsm);
    ASSERT_TRUE(auxPsm->Init(CreateJoinSchema(joinTableName), options, indexPath, generationStr));

    INDEXLIB_TEST_TRUE(auxPsm->Transfer(BUILD_FULL, fullDocs, "", ""));
    if (tableName2Part.find(joinTableName) == tableName2Part.end()) {
        tableName2Part[joinTableName] = partVec.size();
        partVec.push_back(auxPsm->GetIndexPartition());
    } else {
        partVec[tableName2Part[joinTableName]] = auxPsm->GetIndexPartition();
    }
}

void AuxTableInteTest::TestMultiApplication()
{
    PartitionStateMachinePtr psm = PartitionStateMachinePtr(new PartitionStateMachine);
    vector<IndexPartitionPtr> partVec;
    map<string, int> tableName2Part;
    // prepare main tables, aux tables
    {
        string mainTableName = "mainTableA";
        string fullDocs = MakeMainDocs({1, 2, 3}, 1);
        string generationStr = "/MultiApplication/";
        string indexPath = mMainRoot + generationStr + mainTableName;
        ASSERT_TRUE(psm->Init(CreateMainSchema(mainTableName, false),
                              util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam()),
                              indexPath, generationStr));
        INDEXLIB_TEST_TRUE(psm->Transfer(BUILD_FULL, fullDocs, "", ""));
        partVec.push_back(psm->GetIndexPartition());
    }

    // first: test join result right
    IndexApplicationPtr app(new IndexApplication);
    IndexApplicationPtr app2(new IndexApplication);
    JoinRelationMap joinRelations;
    ASSERT_TRUE(app->Init(partVec, joinRelations));
    AssertPkQuery(app, {1, 2, 3}, {0, 1, 2}, "mainTableA");
    ASSERT_TRUE(app2->Init(partVec, joinRelations));
    AssertPkQuery(app2, {1, 2, 3}, {0, 1, 2}, "mainTableA");
    BuildMainTable(psm);
    AssertPkQuery(app2, {4, 5, 6}, {3, 4, 5}, "mainTableA");
    AssertPkQuery(app, {4, 5, 6}, {3, 4, 5}, "mainTableA");
}

void AuxTableInteTest::TestSameVirtualAttribute()
{
    PartitionStateMachinePtr psm(new PartitionStateMachine);
    PartitionStateMachinePtr auxPsm(new PartitionStateMachine);
    vector<IndexPartitionPtr> partVec;
    const string& mainTableName = "mainTableA";
    const string& joinTableName = "AuxTableA";
    // prepare main tables, aux tables
    {
        string fullDocs = MakeMainDocs({1, 2, 3}, 1);
        string generationStr = "/MultiApplication/";
        string indexPath = mMainRoot + generationStr + mainTableName;
        ASSERT_TRUE(psm->Init(CreateMainSchema(mainTableName, false),
                              util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam()),
                              indexPath, generationStr));
        INDEXLIB_TEST_TRUE(psm->Transfer(BUILD_FULL, fullDocs, "", ""));
        partVec.push_back(psm->GetIndexPartition());

        fullDocs = MakeAddCmdString({3, 2, 1}, {3, 2, 1}, 1);
        indexPath = mAuxRoot + generationStr + joinTableName;
        ASSERT_TRUE(auxPsm->Init(CreateJoinSchema(joinTableName),
                                 util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam()),
                                 indexPath, generationStr));
        INDEXLIB_TEST_TRUE(auxPsm->Transfer(BUILD_FULL, fullDocs, "", ""));
        partVec.push_back(auxPsm->GetIndexPartition());
    }

    // first: test join result right
    IndexApplicationPtr app(new IndexApplication);
    IndexApplicationPtr app2(new IndexApplication);
    JoinRelationMap joinRelations;
    joinRelations[mainTableName] = {JoinField("field0", joinTableName, true, true)};
    ASSERT_TRUE(app->Init(partVec, joinRelations));
    AssertPkQuery(app, {1, 2, 3}, {0, 1, 2}, "mainTableA");
    ASSERT_TRUE(app2->Init(partVec, joinRelations));
    AssertPkQuery(app2, {1, 2, 3}, {0, 1, 2}, "mainTableA");
    app.reset();
    BuildMainTable(psm);
    AssertPkQuery(app2, {4, 5, 6}, {3, 4, 5}, "mainTableA");
}

void AuxTableInteTest::TestUnloadAuxTable()
{
    vector<PartitionStateMachinePtr> mainPsms;
    vector<PartitionStateMachinePtr> auxPsms;
    vector<IndexPartitionPtr> partVec;
    map<string, int> tableName2Part;
    string mainTableName = "mainTableA";
    string auxTableName1 = "auxTableA";
    string auxTableName2 = "auxTableB";
    string fullDocs = MakeAddCmdString({3, 2, 1}, {3, 2, 1}, 1);
    UpdateMainFull(mainPsms, partVec, tableName2Part, mainTableName, MakeMainDocs({1, 2, 3}, 1));
    tableName2Part.clear();
    UpdateAuxFull(auxPsms, partVec, tableName2Part, auxTableName1, fullDocs);
    tableName2Part.clear();
    UpdateAuxFull(auxPsms, partVec, tableName2Part, auxTableName2, fullDocs);

    IndexApplicationPtr app(new IndexApplication);
    IndexApplicationPtr app2(new IndexApplication);
    JoinRelationMap joinRelations;
    joinRelations[mainTableName] = {JoinField("field0", auxTableName1, true, true),
                                    JoinField("field1", auxTableName2, true, true)};
    ASSERT_TRUE(app->Init(partVec, joinRelations));
    AssertPkQuery(app, {1, 2, 3}, {0, 1, 2}, "mainTableA");
    joinRelations[mainTableName] = {JoinField("field0", auxTableName1, true, true)};
    partVec.pop_back(); // erase auxtable2
    ASSERT_TRUE(app2->Init(partVec, joinRelations));
    AssertPkQuery(app2, {1, 2, 3}, {0, 1, 2}, "mainTableA");
    app.reset();
    BuildMainTable(mainPsms.back());
    AssertPkQuery(app2, {4, 5, 6}, {3, 4, 5}, "mainTableA");
}
}} // namespace indexlib::partition
