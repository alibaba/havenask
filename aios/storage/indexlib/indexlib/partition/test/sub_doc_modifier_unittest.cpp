#include "autil/StringUtil.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/normal/attribute/accessor/join_docid_attribute_reader.h"
#include "indexlib/index/primary_key/PrimaryKeyReader.h"
#include "indexlib/index/test/document_maker.h"
#include "indexlib/index_base/schema_rewriter.h"
#include "indexlib/partition/modifier/main_sub_modifier_util.h"
#include "indexlib/partition/modifier/sub_doc_modifier.h"
#include "indexlib/partition/test/mock_partition_modifier.h"
#include "indexlib/test/partition_data_maker.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/test/single_field_partition_data_provider.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace autil;
using namespace testing;

using namespace indexlib::test;
using namespace indexlib::config;
using namespace indexlib::common;
using namespace indexlib::document;
using namespace indexlib::index;
using namespace indexlib::index_base;
using namespace indexlib::util;
using namespace indexlib::file_system;

namespace indexlib { namespace partition {

namespace {
class MockSubDocModifier : public SubDocModifier
{
public:
    MockSubDocModifier(const config::IndexPartitionSchemaPtr& schema, const PartitionModifierPtr& mainModifier,
                       const PartitionModifierPtr& subModifier)
        : SubDocModifier(schema)
    {
        mMainModifier = mainModifier;
        mSubModifier = subModifier;
    }
};

class MockPrimaryKeyIndexReader : public indexlibv2::index::UInt64PrimaryKeyReader
{
public:
    MOCK_METHOD(docid_t, Lookup, (const std::string& pkString, future_lite::Executor* executor), (const, override));
};
DEFINE_SHARED_PTR(MockPrimaryKeyIndexReader);

} // namespace

class SubDocModifierTest : public INDEXLIB_TESTBASE
{
public:
    SubDocModifierTest() {}
    ~SubDocModifierTest() {}

    DECLARE_CLASS_NAME(SubDocModifierTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override {}

private:
    SubDocModifierPtr CreateModifier();
    index::JoinDocidAttributeReaderPtr CreateJoinDocidAttributeReader(std::string mainInc, std::string subInc);

    bool CheckGetSubDocIdRange(std::string mainJoinStr, std::string subJoinStr, docid_t docid, docid_t expectBegin,
                               docid_t expectEnd);
    void TEST_RemoveDupSubDocument(SubDocModifierPtr modifier, document::NormalDocumentPtr doc);

private:
    config::IndexPartitionSchemaPtr mSchema;
    config::IndexPartitionSchemaPtr mSubSchema;
    MockPartitionModifierPtr mMainModifier;
    MockPartitionModifierPtr mSubModifier;
    SubDocModifierPtr mModifier;

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(partition, SubDocModifierTest);

void SubDocModifierTest::CaseSetUp()
{
    file_system::FileSystemOptions fsOptions;
    fsOptions.isOffline = false;
    fsOptions.outputStorage = file_system::FSST_MEM;
    RESET_FILE_SYSTEM("ut", false, fsOptions);

    string field = "price:int32;pk:int64";
    string index = "pk:primarykey64:pk";
    string attr = "price;pk";
    string summary = "";
    mSchema = SchemaMaker::MakeSchema(field, index, attr, summary);
    IndexPartitionSchemaPtr subSchema =
        SchemaMaker::MakeSchema("sub_pk:string;sub_long:uint32;", "sub_pk:primarykey64:sub_pk", "sub_pk;sub_long;", "");

    auto indexOptions = IndexPartitionOptions();
    indexOptions.SetIsOnline(false);

    mSchema->SetSubIndexPartitionSchema(subSchema);
    SchemaRewriter::Rewrite(mSchema, indexOptions, GET_PARTITION_DIRECTORY());
    mSubSchema = subSchema;

    mMainModifier.reset(new MockPartitionModifier(mSchema));
    mSubModifier.reset(new MockPartitionModifier(mSubSchema));
    mModifier.reset(new MockSubDocModifier(mSchema, mMainModifier, mSubModifier));
}

void SubDocModifierTest::TEST_RemoveDupSubDocument(SubDocModifierPtr modifier, document::NormalDocumentPtr doc)
{
    modifier->RemoveDupSubDocument(doc);
}

TEST_F(SubDocModifierTest, TestRemoveDupSubDocument)
{
    document::NormalDocumentPtr doc = DocumentMaker::MakeDeletionDocument("10", 100);
    document::NormalDocumentPtr subDoc = DocumentMaker::MakeDeletionDocument("101", 100);
    doc->AddSubDocument(subDoc);
    doc->AddSubDocument(subDoc);

    EXPECT_CALL(*mSubModifier, RemoveDocument(An<const DocumentPtr&>())).Times(2);
    TEST_RemoveDupSubDocument(mModifier, doc);
}

TEST_F(SubDocModifierTest, TestGetSubDocIdRange)
{
    ASSERT_TRUE(CheckGetSubDocIdRange("2", "0,0", 0, 0, 2));
    ASSERT_THROW(CheckGetSubDocIdRange("-1", "0,0", 0, 0, 2), IndexCollapsedException);
    ASSERT_TRUE(CheckGetSubDocIdRange("2,4", "0,0,1,1", 1, 2, 4));
    ASSERT_TRUE(CheckGetSubDocIdRange("4,4", "0,0,1,1", 1, 4, 4));
    ASSERT_THROW(CheckGetSubDocIdRange("-1,4", "0,0,1,1", 1, -1, 4), IndexCollapsedException);
    ASSERT_THROW(CheckGetSubDocIdRange("5,4", "0,0,1,1", 1, 4, 4), IndexCollapsedException);
}

bool SubDocModifierTest::CheckGetSubDocIdRange(string mainJoinStr, string subJoinStr, docid_t docid,
                                               docid_t expectBegin, docid_t expectEnd)
{
    tearDown();
    setUp();
    JoinDocidAttributeReaderPtr reader = CreateJoinDocidAttributeReader(mainJoinStr, subJoinStr);
    docid_t subDocIdBegin = INVALID_DOCID;
    docid_t subDocIdEnd = INVALID_DOCID;
    MainSubModifierUtil::GetSubDocIdRange(reader.get(), docid, &subDocIdBegin, &subDocIdEnd);
    if ((subDocIdBegin != expectBegin) || (subDocIdEnd != expectEnd)) {
        EXPECT_EQ((docid_t)0, subDocIdBegin);
        EXPECT_EQ((docid_t)2, subDocIdEnd);
        return false;
    }
    return true;
}

TEST_F(SubDocModifierTest, TestIsDirty)
{
    // main dirty
    EXPECT_CALL(*mMainModifier, IsDirty()).WillOnce(Return(true));
    ASSERT_TRUE(mModifier->IsDirty());
    // sub dirty
    EXPECT_CALL(*mMainModifier, IsDirty()).WillOnce(Return(false));
    EXPECT_CALL(*mSubModifier, IsDirty()).WillOnce(Return(true));
    ASSERT_TRUE(mModifier->IsDirty());
    // non dirty
    EXPECT_CALL(*mMainModifier, IsDirty()).WillOnce(Return(false));
    EXPECT_CALL(*mSubModifier, IsDirty()).WillOnce(Return(false));
    ASSERT_FALSE(mModifier->IsDirty());
}

JoinDocidAttributeReaderPtr SubDocModifierTest::CreateJoinDocidAttributeReader(string mainInc, string subInc)
{
    // string mainRoot = GET_PARTITION_DIRECTORY()->MakeDirectory("main")->GetLogicalPath();
    auto mainRoot = PathUtil::JoinPath(GET_TEMP_DATA_PATH(), "main");
    auto ec = FslibWrapper::MkDir(mainRoot).Code();
    if (ec != FSEC_OK) {
        return nullptr;
    }

    SingleFieldPartitionDataProviderPtr mainProvider(new SingleFieldPartitionDataProvider);
    mainProvider->Init(mainRoot, "int32", SFP_ATTRIBUTE);
    mainProvider->Build(mainInc, SFP_OFFLINE);
    PartitionDataPtr partData = mainProvider->GetPartitionData();
    AttributeConfigPtr attrConfig = mainProvider->GetAttributeConfig();
    JoinDocidAttributeReaderPtr reader(new JoinDocidAttributeReader);
    reader->Open(attrConfig, partData, PatchApplyStrategy::PAS_APPLY_NO_PATCH);

    // string subRoot = GET_PARTITION_DIRECTORY()->MakeDirectory("sub")->GetLogicalPath();
    auto subRoot = PathUtil::JoinPath(GET_TEMP_DATA_PATH(), "sub");
    ec = FslibWrapper::MkDir(subRoot).Code();
    if (ec != FSEC_OK) {
        return nullptr;
    }

    SingleFieldPartitionDataProviderPtr subProvider(new SingleFieldPartitionDataProvider);
    subProvider->Init(subRoot, "int32", SFP_ATTRIBUTE);
    subProvider->Build(subInc, SFP_OFFLINE);
    PartitionDataPtr subPartData = subProvider->GetPartitionData();
    reader->InitJoinBaseDocId(subPartData);

    return reader;
}

TEST_F(SubDocModifierTest, TestInit)
{
    PartitionDataPtr partitionData =
        PartitionDataMaker::CreatePartitionData(GET_FILE_SYSTEM(), IndexPartitionOptions(), mSchema);

    SubDocModifier modifier(mSchema);
    modifier.Init(partitionData, false, false);

    EXPECT_TRUE(modifier.GetMainModifier());
    EXPECT_TRUE(modifier.GetSubModifier());

    const util::BuildResourceMetricsPtr& mainBuildMetrics = modifier.GetMainModifier()->GetBuildResourceMetrics();
    const util::BuildResourceMetricsPtr& subBuildMetrics = modifier.GetSubModifier()->GetBuildResourceMetrics();

    ASSERT_TRUE(mainBuildMetrics);
    ASSERT_TRUE(subBuildMetrics);

    ASSERT_EQ(mainBuildMetrics, subBuildMetrics);
}
}} // namespace indexlib::partition
