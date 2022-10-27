#include <autil/StringUtil.h>
#include "indexlib/partition/test/sub_doc_modifier_unittest.h"
#include "indexlib/index/normal/primarykey/primary_key_index_reader_typed.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/test/single_field_partition_data_provider.h"
#include "indexlib/test/partition_data_maker.h"
#include "indexlib/index/test/document_maker.h"
#include "indexlib/index_base/schema_rewriter.h"

using namespace std;
using namespace autil;
using namespace testing;

IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, SubDocModifierTest);

namespace
{
class MockSubDocModifier : public SubDocModifier
{
public:
    MockSubDocModifier(const config::IndexPartitionSchemaPtr& schema,
                       const PartitionModifierPtr& mainModifier,
                       const PartitionModifierPtr& subModifier)
        : SubDocModifier(schema)
    {
        mMainModifier = mainModifier;
        mSubModifier = subModifier;
    }
};

class MockPrimaryKeyIndexReader : public UInt64PrimaryKeyIndexReader
{
public:
    MOCK_CONST_METHOD1(Lookup, docid_t(const std::string& pkString));
};
DEFINE_SHARED_PTR(MockPrimaryKeyIndexReader);

}

SubDocModifierTest::SubDocModifierTest()
{
}

SubDocModifierTest::~SubDocModifierTest()
{
}

void SubDocModifierTest::CaseSetUp()
{
    string field = "price:int32;pk:int64";
    string index = "pk:primarykey64:pk";
    string attr = "price;pk";
    string summary = "";
    mSchema = SchemaMaker::MakeSchema(field, index, attr, summary);
    IndexPartitionSchemaPtr subSchema = SchemaMaker::MakeSchema(
            "sub_pk:string;sub_long:uint32;",
            "sub_pk:primarykey64:sub_pk",
            "sub_pk;sub_long;",
            "");
    mSchema->SetSubIndexPartitionSchema(subSchema);
    SchemaRewriter::Rewrite(mSchema, IndexPartitionOptions(), GET_TEST_DATA_PATH());
    mSubSchema = subSchema;

    mMainModifier.reset(new MockPartitionModifier(mSchema));
    mSubModifier.reset(new MockPartitionModifier(mSubSchema));
    mModifier.reset(new MockSubDocModifier(mSchema, mMainModifier, mSubModifier));

    mDoc = DocumentMaker::MakeDeletionDocument("10", 100);
    mSubDoc = DocumentMaker::MakeDeletionDocument("101", 100);
    mDoc->AddSubDocument(mSubDoc);
}

void SubDocModifierTest::CaseTearDown()
{
}

void SubDocModifierTest::TestRemoveDupSubDocument()
{
    mDoc->AddSubDocument(mSubDoc);
    EXPECT_CALL(*mSubModifier, RemoveDocument(An<const DocumentPtr&>()))
        .Times(2);
    mModifier->RemoveDupSubDocument(mDoc);
}

void SubDocModifierTest::TestNeedUpdate()
{
    fieldid_t pkFieldId = mSchema->GetIndexSchema()->GetPrimaryKeyIndexFieldId();

    // empty
    std::vector<fieldid_t> fieldIds;
    std::vector<std::string> fieldValues;
    NormalDocumentPtr doc = DocumentMaker::MakeUpdateDocument(
            mSchema, "1", fieldIds, fieldValues, 10);
    ASSERT_FALSE(mModifier->NeedUpdate(doc, pkFieldId));

    // pk attribute only
    fieldIds.push_back(pkFieldId);
    fieldValues.push_back("1");
    doc = DocumentMaker::MakeUpdateDocument(mSchema, "1", fieldIds, fieldValues, 10);
    ASSERT_FALSE(mModifier->NeedUpdate(doc, pkFieldId));

    // has attribute field and pkAttr
    fieldIds.push_back(mSchema->GetFieldSchema()->GetFieldId("price"));
    fieldValues.push_back("111");
    doc = DocumentMaker::MakeUpdateDocument(mSchema, "1", fieldIds, fieldValues, 10);
    ASSERT_TRUE(mModifier->NeedUpdate(doc, pkFieldId));

    // no attribute document
    doc->SetAttributeDocument(AttributeDocumentPtr());
    ASSERT_FALSE(mModifier->NeedUpdate(doc, pkFieldId));
}

void SubDocModifierTest::TestValidateSubDocsIfMainDocIdInvalid()
{
    MockPrimaryKeyIndexReader *mockMainPkIndexReader = new MockPrimaryKeyIndexReader;
    EXPECT_CALL(*mockMainPkIndexReader, Lookup(_))
        .WillOnce(Return(INVALID_DOCID));
    mMainModifier->SetPrimaryKeyIndexReader(PrimaryKeyIndexReaderPtr(mockMainPkIndexReader));

    ASSERT_FALSE(mModifier->ValidateSubDocs(mDoc));
}

void SubDocModifierTest::TestValidateSubDocs()
{
    MockPrimaryKeyIndexReader *mockMainPkIndexReader = new MockPrimaryKeyIndexReader;
    EXPECT_CALL(*mockMainPkIndexReader, Lookup(_))
        .WillOnce(Return(0));
    mMainModifier->SetPrimaryKeyIndexReader(PrimaryKeyIndexReaderPtr(mockMainPkIndexReader));

    MockPrimaryKeyIndexReader *mockSubPkIndexReader = new MockPrimaryKeyIndexReader;
    EXPECT_CALL(*mockSubPkIndexReader, Lookup("101")).WillOnce(Return(INVALID_DOCID));
    EXPECT_CALL(*mockSubPkIndexReader, Lookup("102")).WillOnce(Return(1));
    EXPECT_CALL(*mockSubPkIndexReader, Lookup("103")).WillOnce(Return(10));
    mSubModifier->SetPrimaryKeyIndexReader(PrimaryKeyIndexReaderPtr(mockSubPkIndexReader));

    // Main 0 ~ Sub[0,2)
    mModifier->mMainJoinAttributeReader = CreateJoinDocidAttributeReader("2", "0,0");
    // make it has 3 sub doc: 101(INVALID_DOCID,ok), 102(1,ok), 103(10,remove)
    NormalDocumentPtr subDoc1 = DocumentMaker::MakeDeletionDocument("102", 100);
    mDoc->AddSubDocument(subDoc1);
    NormalDocumentPtr subDoc2 = DocumentMaker::MakeDeletionDocument("103", 100);
    mDoc->AddSubDocument(subDoc2);

    ASSERT_TRUE(mModifier->ValidateSubDocs(mDoc));
    ASSERT_EQ((size_t)2, mDoc->GetSubDocuments().size());
}

void SubDocModifierTest::TestGetSubDocIdRange()
{
    ASSERT_TRUE(CheckGetSubDocIdRange("2", "0,0", 0, 0, 2));
    ASSERT_THROW(CheckGetSubDocIdRange("-1", "0,0", 0, 0, 2),
                 IndexCollapsedException);
    ASSERT_TRUE(CheckGetSubDocIdRange("2,4", "0,0,1,1", 1, 2, 4));
    ASSERT_TRUE(CheckGetSubDocIdRange("4,4", "0,0,1,1", 1, 4, 4));
    ASSERT_THROW(CheckGetSubDocIdRange("-1,4", "0,0,1,1", 1, -1, 4),
                 IndexCollapsedException);
    ASSERT_THROW(CheckGetSubDocIdRange("5,4", "0,0,1,1", 1, 4, 4),
                 IndexCollapsedException);
}

bool SubDocModifierTest::CheckGetSubDocIdRange(string mainJoinStr, string subJoinStr,
        docid_t docid, docid_t expectBegin, docid_t expectEnd)
{
    TearDown();
    SetUp();
    mModifier->mMainJoinAttributeReader =
        CreateJoinDocidAttributeReader(mainJoinStr, subJoinStr);
    docid_t subDocIdBegin = INVALID_DOCID;
    docid_t subDocIdEnd = INVALID_DOCID;
    mModifier->GetSubDocIdRange(docid, subDocIdBegin, subDocIdEnd);
    if ((subDocIdBegin != expectBegin) || (subDocIdEnd != expectEnd))
    {
        EXPECT_EQ((docid_t)0, subDocIdBegin);
        EXPECT_EQ((docid_t)2, subDocIdEnd);
        return false;
    }
    return true;
}

void SubDocModifierTest::TestIsDirty()
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

JoinDocidAttributeReaderPtr SubDocModifierTest::CreateJoinDocidAttributeReader(
        string mainInc, string subInc)
{
    string mainRoot = GET_PARTITION_DIRECTORY()->MakeDirectory("main")->GetPath();
    SingleFieldPartitionDataProviderPtr mainProvider(new SingleFieldPartitionDataProvider);
    mainProvider->Init(mainRoot, "int32", SFP_ATTRIBUTE);
    mainProvider->Build(mainInc, SFP_OFFLINE);
    PartitionDataPtr partData = mainProvider->GetPartitionData();
    AttributeConfigPtr attrConfig = mainProvider->GetAttributeConfig();
    JoinDocidAttributeReaderPtr reader(new JoinDocidAttributeReader);
    reader->Open(attrConfig, partData);

    string subRoot = GET_PARTITION_DIRECTORY()->MakeDirectory("sub")->GetPath();
    SingleFieldPartitionDataProviderPtr subProvider(new SingleFieldPartitionDataProvider);
    subProvider->Init(subRoot, "int32", SFP_ATTRIBUTE);
    subProvider->Build(subInc, SFP_OFFLINE);
    PartitionDataPtr subPartData = subProvider->GetPartitionData();
    reader->InitJoinBaseDocId(subPartData);

    return reader;
}

/*
void SubDocModifierTest::TestUpdateDocumentWithNoField()
{
    fieldid_t pkFieldId = mSchema->GetIndexSchema()->GetPrimaryKeyIndexFieldId();
    vector<fieldid_t> fieldIds(1, pkFieldId);
    vector<string> fieldValues(1, "1");
    DocumentPtr doc = DocumentMaker::MakeUpdateDocument(mSchema, "1", fieldIds, fieldValues, 10);

    // prepare for GetDocId and GetSubDocIdRange, mock it?
    MockPrimaryKeyIndexReader *mockMainPkIndexReader = new MockPrimaryKeyIndexReader;
    EXPECT_CALL(*mockMainPkIndexReader, Lookup(_)).WillOnce(Return(0));
    EXPECT_CALL(*mSubModifier, UpdateDocument(_)).WillOnce(Return(false));
    mMainModifier->SetPrimaryKeyIndexReader(PrimaryKeyIndexReaderPtr(mockMainPkIndexReader));
    mModifier->mMainJoinAttributeReader = CreateJoinDocidAttributeReader("2", "0,0");

    ASSERT_FALSE(mModifier->UpdateDocument(doc));
}
*/

void SubDocModifierTest::TestInit()
{
    PartitionDataPtr partitionData = PartitionDataMaker::CreatePartitionData(
            GET_FILE_SYSTEM(), IndexPartitionOptions(), mSchema);

    SubDocModifier modifier(mSchema);
    modifier.Init(partitionData, false, false);

    EXPECT_TRUE(modifier.mMainModifier);
    EXPECT_TRUE(modifier.mSubModifier);

    const util::BuildResourceMetricsPtr& mainBuildMetrics =
        modifier.mMainModifier->GetBuildResourceMetrics();

    const util::BuildResourceMetricsPtr& subBuildMetrics =
        modifier.mSubModifier->GetBuildResourceMetrics();

    ASSERT_TRUE(mainBuildMetrics);
    ASSERT_TRUE(subBuildMetrics);

    ASSERT_EQ(mainBuildMetrics, subBuildMetrics);
}

IE_NAMESPACE_END(partition);
