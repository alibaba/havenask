#include "build_service/builder/SortDocumentContainer.h"

#include <cstddef>
#include <memory>
#include <stdint.h>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "autil/Span.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/mem_pool/PoolVector.h"
#include "build_service/builder/NormalSortDocSorter.h"
#include "build_service/builder/SortDocumentSorter.h"
#include "build_service/common/Locator.h"
#include "build_service/document/DocumentDefine.h"
#include "build_service/document/test/DocumentTestHelper.h"
#include "build_service/test/unittest.h"
#include "indexlib/base/FieldType.h"
#include "indexlib/base/Progress.h"
#include "indexlib/base/Types.h"
#include "indexlib/config/SortDescription.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/config/attribute_schema.h"
#include "indexlib/config/field_config.h"
#include "indexlib/config/index_config.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/index_schema.h"
#include "indexlib/config/primary_key_index_config.h"
#include "indexlib/config/region_schema.h"
#include "indexlib/config/schema_modify_operation.h"
#include "indexlib/config/test/schema_maker.h"
#include "indexlib/document/document.h"
#include "indexlib/document/index_document/normal_document/attribute_document.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/document/index_locator.h"
#include "indexlib/document/locator.h"
#include "indexlib/document/normal/AttributeDocument.h"
#include "indexlib/index/common/Types.h"
#include "indexlib/index/inverted_index/config/AdaptiveDictionaryConfig.h"
#include "indexlib/index/kv/Constant.h"
#include "indexlib/index_base/index_meta/partition_meta.h"
#include "indexlib/indexlib.h"
#include "indexlib/misc/common.h"
#include "indexlib/test/document_creator.h"
#include "indexlib/util/ErrorLogCollector.h"
#include "unittest/unittest.h"

using namespace indexlib::document;
using namespace indexlib::index_base;
using namespace indexlib::config;

using namespace indexlib;
using namespace autil;
using namespace build_service::document;
namespace build_service { namespace builder {

using namespace std;
using namespace testing;

class SortDocumentContainerTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp();
    void tearDown();

private:
    indexlib::document::DocumentPtr createDocument(docid_t docId, const std::string& pkStr = "",
                                                   DocOperateType opType = ADD_DOC, uint32_t sortField = 0,
                                                   int64_t locator = 0, bool isNull = false);

    // sortFieldInfo: fieldType=fieldValue  example: date=2010-01-01
    indexlib::document::DocumentPtr createDocument(docid_t docId, const std::string& sortFieldInfo,
                                                   const std::string& pkStr = "", DocOperateType opType = ADD_DOC,
                                                   int64_t locator = 0);

    void SetPKToSchema(const IndexPartitionSchemaPtr& schema)
    {
        PrimaryKeyIndexConfigPtr pkConfig(new PrimaryKeyIndexConfig("pk", it_primarykey64));
        auto regionSchema = schema->GetRegionSchema(DEFAULT_REGIONID);
        regionSchema->ResetIndexSchema();
        regionSchema->GetIndexSchema()->SetPrimaryKeyIndexConfig(pkConfig);
    }

    void resetSchema(FieldType fieldType = ft_uint32)
    {
        _sortDescs.push_back(indexlibv2::config::SortDescription("field", indexlibv2::config::sp_asc));
        _schema.reset(new IndexPartitionSchema());
        RegionSchemaPtr regionSchema;
        regionSchema.reset(new RegionSchema(_schema->mImpl.get()));
        {
            FieldConfigPtr fieldConfig(new FieldConfig("field", fieldType, false));
            fieldConfig->SetFieldId(0);
            AttributeConfigPtr attrConfig(new AttributeConfig);
            attrConfig->Init(fieldConfig);
            regionSchema->AddAttributeConfig(attrConfig);
        }
        {
            FieldConfigPtr fieldConfig(new FieldConfig("uint1", ft_uint32, false));
            fieldConfig->SetFieldId(1);
            AttributeConfigPtr attrConfig(new AttributeConfig);
            attrConfig->Init(fieldConfig);
            regionSchema->AddAttributeConfig(attrConfig);
        }
        {
            FieldConfigPtr fieldConfig(new FieldConfig("uint2", ft_uint32, false));
            fieldConfig->SetFieldId(2);
            AttributeConfigPtr attrConfig(new AttributeConfig);
            attrConfig->Init(fieldConfig);
            regionSchema->AddAttributeConfig(attrConfig);
        }
        _attrSchema = regionSchema->GetAttributeSchema();
        _schema->AddRegionSchema(regionSchema);
    }

protected:
    AttributeSchemaPtr _attrSchema;
    IndexPartitionSchemaPtr _schema;
    indexlibv2::config::SortDescriptions _sortDescs;
};

void SortDocumentContainerTest::setUp() { resetSchema(); }

void SortDocumentContainerTest::tearDown() {}

// use docid to check docs' order

TEST_F(SortDocumentContainerTest, testPushWithoutPk)
{
    SortDocumentContainer container;
    common::Locator locator;
    ASSERT_TRUE(container.init(_sortDescs, _schema, locator));

    container.pushDocument(createDocument(0, "1", ADD_DOC, 0));
    container.pushDocument(createDocument(1, "1", ADD_DOC, 1));
    container.pushDocument(createDocument(2, "1", UPDATE_FIELD, 1));
    EXPECT_EQ(size_t(3), container.allDocCount());
    container._orderVec[0] = 0;
    container._orderVec[1] = 1;
    container._orderVec[2] = 2;
    container.sortDocument();
    EXPECT_EQ(int64_t(0), container[0]->GetTimestamp());
    EXPECT_EQ(int64_t(1), container[1]->GetTimestamp());
    EXPECT_EQ(int64_t(2), container[2]->GetTimestamp());
    NormalSortDocSorter* sorter = dynamic_cast<NormalSortDocSorter*>(container._sortDocumentSorter.get());
    ASSERT_TRUE(sorter);
    EXPECT_TRUE(sorter->_documentPkPosMap->empty());
    EXPECT_TRUE(sorter->_mergePosVector->empty());
}

TEST_F(SortDocumentContainerTest, testPushwithPk)
{
    // 1. same pk with different operate type
    // 2. the recent ADD doc overwrite ADD before
    // 3. UPDATE, DELETE comes without ADD
    SortDocumentContainer container;
    SetPKToSchema(_schema);
    common::Locator locator;
    ASSERT_TRUE(container.init(_sortDescs, _schema, locator));

    container.pushDocument(createDocument(0, "1", ADD_DOC, 0));
    container.pushDocument(createDocument(1, "2", ADD_DOC, 1));
    container.pushDocument(createDocument(2, "1", UPDATE_FIELD, 0));
    container.pushDocument(createDocument(3, "1", DELETE_DOC, 0));
    container.pushDocument(createDocument(4, "1", ADD_DOC, 0));
    container.pushDocument(createDocument(5, "3", UPDATE_FIELD, 0));

    EXPECT_EQ(size_t(6), container.allDocCount());
    NormalSortDocSorter* sorter = dynamic_cast<NormalSortDocSorter*>(container._sortDocumentSorter.get());
    ASSERT_TRUE(sorter);

    EXPECT_THAT((*(sorter->_documentPkPosMap)), UnorderedElementsAre(Pair(StringView("1"), pair<int, int>(4, -1)),
                                                                     Pair(StringView("2"), pair<int, int>(1, -1)),
                                                                     Pair(StringView("3"), pair<int, int>(5, -1))));
    ASSERT_EQ(size_t(1), sorter->_mergePosVector->size());
    EXPECT_THAT((*(sorter->_mergePosVector))[0], ElementsAre(2, 3));
}

TEST_F(SortDocumentContainerTest, testSortWithoutPk)
{
    // 1. sort stable with equal sort field
    // 2. ignore same pk with different operate type
    SortDocumentContainer container;
    _sortDescs[0].TEST_SetSortPattern(indexlibv2::config::sp_desc);
    common::Locator locator;
    ASSERT_TRUE(container.init(_sortDescs, _schema, locator));

    container.pushDocument(createDocument(0, "1", ADD_DOC, 0));
    container.pushDocument(createDocument(1, "2", ADD_DOC, 1));
    container.pushDocument(createDocument(2, "2", ADD_DOC, 1));
    container.pushDocument(createDocument(3, "3", ADD_DOC, 2));
    container.sortDocument();
    ASSERT_EQ(size_t(4), container.allDocCount());
    ASSERT_EQ(size_t(4), container.sortedDocCount());
    EXPECT_EQ(int64_t(3), container[0]->GetTimestamp());
    EXPECT_EQ(int64_t(1), container[1]->GetTimestamp());
    EXPECT_EQ(int64_t(2), container[2]->GetTimestamp());
    EXPECT_EQ(int64_t(0), container[3]->GetTimestamp());
}

TEST_F(SortDocumentContainerTest, testSortWithTimeAttribute)
{
    {
        resetSchema(ft_date);
        SortDocumentContainer container;
        _sortDescs[0].TEST_SetSortPattern(indexlibv2::config::sp_desc);
        common::Locator locator;
        ASSERT_TRUE(container.init(_sortDescs, _schema, locator));

        container.pushDocument(createDocument(0, "date=2000-01-01", "1", ADD_DOC));
        container.pushDocument(createDocument(1, "date=2000-01-02", "2", ADD_DOC));
        container.pushDocument(createDocument(2, "date=2000-01-02", "2", ADD_DOC));
        container.pushDocument(createDocument(3, "date=2000-01-03", "3", ADD_DOC));
        container.sortDocument();
        ASSERT_EQ(size_t(4), container.allDocCount());
        ASSERT_EQ(size_t(4), container.sortedDocCount());
        EXPECT_EQ(int64_t(3), container[0]->GetTimestamp());
        EXPECT_EQ(int64_t(1), container[1]->GetTimestamp());
        EXPECT_EQ(int64_t(2), container[2]->GetTimestamp());
        EXPECT_EQ(int64_t(0), container[3]->GetTimestamp());
    }
    {
        resetSchema(ft_time);
        SortDocumentContainer container;
        _sortDescs[0].TEST_SetSortPattern(indexlibv2::config::sp_desc);
        common::Locator locator;
        ASSERT_TRUE(container.init(_sortDescs, _schema, locator));

        container.pushDocument(createDocument(0, "time=20:01:01", "1", ADD_DOC));
        container.pushDocument(createDocument(1, "time=20:01:02", "2", ADD_DOC));
        container.pushDocument(createDocument(2, "time=20:01:02", "2", ADD_DOC));
        container.pushDocument(createDocument(3, "time=20:01:03", "3", ADD_DOC));
        container.sortDocument();
        ASSERT_EQ(size_t(4), container.allDocCount());
        ASSERT_EQ(size_t(4), container.sortedDocCount());
        EXPECT_EQ(int64_t(3), container[0]->GetTimestamp());
        EXPECT_EQ(int64_t(1), container[1]->GetTimestamp());
        EXPECT_EQ(int64_t(2), container[2]->GetTimestamp());
        EXPECT_EQ(int64_t(0), container[3]->GetTimestamp());
    }

    {
        resetSchema(ft_timestamp);
        SortDocumentContainer container;
        _sortDescs[0].TEST_SetSortPattern(indexlibv2::config::sp_desc);
        common::Locator locator;
        ASSERT_TRUE(container.init(_sortDescs, _schema, locator));

        container.pushDocument(createDocument(0, "timestamp=2000-01-01 20:01:01", "1", ADD_DOC));
        container.pushDocument(createDocument(1, "timestamp=2000-01-02 20:01:02", "2", ADD_DOC));
        container.pushDocument(createDocument(2, "timestamp=2000-01-02 20:01:02", "2", ADD_DOC));
        container.pushDocument(createDocument(3, "timestamp=2000-01-03 20:01:03", "3", ADD_DOC));
        container.sortDocument();
        ASSERT_EQ(size_t(4), container.allDocCount());
        ASSERT_EQ(size_t(4), container.sortedDocCount());
        EXPECT_EQ(int64_t(3), container[0]->GetTimestamp());
        EXPECT_EQ(int64_t(1), container[1]->GetTimestamp());
        EXPECT_EQ(int64_t(2), container[2]->GetTimestamp());
        EXPECT_EQ(int64_t(0), container[3]->GetTimestamp());
    }
}

TEST_F(SortDocumentContainerTest, testSortWithNull)
{
    {
        SortDocumentContainer container;
        _sortDescs[0].TEST_SetSortPattern(indexlibv2::config::sp_desc);
        common::Locator locator;
        ASSERT_TRUE(container.init(_sortDescs, _schema, locator));
        container.pushDocument(createDocument(0, "1", ADD_DOC, 0));
        container.pushDocument(createDocument(1, "2", ADD_DOC, 1, 0, true));
        container.pushDocument(createDocument(2, "2", ADD_DOC, 8));
        container.pushDocument(createDocument(3, "3", ADD_DOC, 3, 0, true));
        container.pushDocument(createDocument(4, "3", ADD_DOC, 2));
        container.pushDocument(createDocument(5, "3", ADD_DOC, 1));
        container.sortDocument();
        ASSERT_EQ(size_t(6), container.allDocCount());
        ASSERT_EQ(size_t(6), container.sortedDocCount());
        EXPECT_EQ(int64_t(2), container[0]->GetTimestamp());
        EXPECT_EQ(int64_t(4), container[1]->GetTimestamp());
        EXPECT_EQ(int64_t(5), container[2]->GetTimestamp());
        EXPECT_EQ(int64_t(0), container[3]->GetTimestamp());
        EXPECT_EQ(int64_t(1), container[4]->GetTimestamp());
        EXPECT_EQ(int64_t(3), container[5]->GetTimestamp());
    }
    {
        SortDocumentContainer container;
        _sortDescs[0].TEST_SetSortPattern(indexlibv2::config::sp_asc);
        common::Locator locator;
        ASSERT_TRUE(container.init(_sortDescs, _schema, locator));
        container.pushDocument(createDocument(0, "1", ADD_DOC, 0));
        container.pushDocument(createDocument(1, "2", ADD_DOC, 3, 0, true));
        container.pushDocument(createDocument(2, "2", ADD_DOC, 4));
        container.pushDocument(createDocument(3, "3", ADD_DOC, 2, 0, true));
        container.pushDocument(createDocument(4, "3", ADD_DOC, 5));
        container.pushDocument(createDocument(5, "3", ADD_DOC, 1));
        container.sortDocument();
        ASSERT_EQ(size_t(6), container.allDocCount());
        ASSERT_EQ(size_t(6), container.sortedDocCount());
        EXPECT_EQ(int64_t(1), container[0]->GetTimestamp()); // 0
        EXPECT_EQ(int64_t(3), container[1]->GetTimestamp()); // 3
        EXPECT_EQ(int64_t(0), container[2]->GetTimestamp()); // 2
        EXPECT_EQ(int64_t(5), container[3]->GetTimestamp());
        EXPECT_EQ(int64_t(2), container[4]->GetTimestamp());
        EXPECT_EQ(int64_t(4), container[5]->GetTimestamp());
    }
}

TEST_F(SortDocumentContainerTest, testSortKKV)
{
    string fields = "value:int8;pkey:uint64;skey:uint64";
    _schema = test::SchemaMaker::MakeKKVSchema(fields, "pkey", "skey", "value;skey");
    string docStr = "cmd=add,value=8,pkey=1,skey=2,ts=1;"
                    "cmd=add,value=9, pkey=2,skey=1,ts=6;"
                    "cmd=add,value=9, pkey=2,skey=2,ts=5;"
                    "cmd=delete,pkey=1,ts=2;"
                    "cmd=add,value=9, pkey=1,skey=4,ts=3;"
                    "cmd=delete,value=9, pkey=1,skey=2,ts=4;"
                    "cmd=add,value=9, pkey=1,skey=2,ts=7;";

    auto docs = test::DocumentCreator::CreateKVDocuments(_schema, docStr);
    SortDocumentContainer container;
    common::Locator locator;
    ASSERT_TRUE(container.init(_sortDescs, _schema, locator));
    for (size_t i = 0; i < docs.size(); i++) {
        container.pushDocument(docs[i]);
    }
    container.sortDocument();
    ASSERT_EQ((size_t)6, container.sortedDocCount());
    ASSERT_EQ((int64_t)2, container[0]->GetTimestamp());
    ASSERT_EQ((int64_t)3, container[1]->GetTimestamp());
    ASSERT_EQ((int64_t)4, container[2]->GetTimestamp());
    ASSERT_EQ((int64_t)7, container[3]->GetTimestamp());
    ASSERT_EQ((int64_t)5, container[4]->GetTimestamp());
    ASSERT_EQ((int64_t)6, container[5]->GetTimestamp());
}

TEST_F(SortDocumentContainerTest, testSortKV)
{
    string fields = "value:int8;pkey:uint64;skey:uint64";
    _schema = test::SchemaMaker::MakeKVSchema(fields, "pkey", "value;skey");
    string docStr = "cmd=add,value=8,pkey=1,skey=2,ts=1;"
                    "cmd=add,value=9,pkey=2,skey=1,ts=6;"
                    "cmd=add,value=10,pkey=3,skey=2,ts=5;"
                    "cmd=delete,pkey=1,ts=2;"
                    "cmd=add,value=9,pkey=1,skey=4,ts=3;"
                    "cmd=delete,value=9,pkey=2,skey=2,ts=4;"
                    "cmd=add,value=9,pkey=4,skey=6,ts=7;";

    common::Locator locator;

    {
        // sort by field
        auto docs = test::DocumentCreator::CreateKVDocuments(_schema, docStr);
        SortDocumentContainer container;
        indexlibv2::config::SortDescriptions sortDescs;
        indexlibv2::config::SortDescription sortDesc;
        sortDesc.TEST_SetSortFieldName("value");
        sortDesc.TEST_SetSortPattern(indexlibv2::config::sp_asc);
        sortDescs.push_back(sortDesc);

        ASSERT_TRUE(container.init(sortDescs, _schema, locator));
        for (size_t i = 0; i < docs.size(); i++) {
            container.pushDocument(docs[i]);
        }
        container.sortDocument();
        ASSERT_EQ((size_t)5, container.sortedDocCount());
        ASSERT_EQ((int64_t)2, container[0]->GetTimestamp());
        ASSERT_EQ((int64_t)4, container[1]->GetTimestamp());
        ASSERT_EQ((int64_t)3, container[2]->GetTimestamp());
        ASSERT_EQ((int64_t)7, container[3]->GetTimestamp());
        ASSERT_EQ((int64_t)5, container[4]->GetTimestamp());
    }

    {
        // sort by sim hash
        auto docs = test::DocumentCreator::CreateKVDocuments(_schema, docStr);
        SortDocumentContainer container;
        indexlibv2::config::SortDescriptions sortDescs;
        ASSERT_TRUE(container.init(sortDescs, _schema, locator));
        for (size_t i = 0; i < docs.size(); i++) {
            container.pushDocument(docs[i]);
        }
        container.sortDocument();
        ASSERT_EQ((size_t)5, container.sortedDocCount());
        ASSERT_EQ((int64_t)2, container[0]->GetTimestamp());
        ASSERT_EQ((int64_t)4, container[1]->GetTimestamp());
        ASSERT_EQ((int64_t)5, container[2]->GetTimestamp());
        ASSERT_EQ((int64_t)3, container[3]->GetTimestamp());
        ASSERT_EQ((int64_t)7, container[4]->GetTimestamp());
    }
}

TEST_F(SortDocumentContainerTest, testSortKKVWithSameSkey)
{
    string fields = "value:int8;pkey:uint64;skey:uint64";
    _schema = test::SchemaMaker::MakeKKVSchema(fields, "pkey", "skey", "value;skey");
    size_t num = 43;
    for (size_t i = 0; i < num; i++) {
        string docStr = "";
        for (size_t j = 0; j < i; j++) {
            docStr = docStr + "cmd=add,value=8,pkey=2,skey=2,ts=" + StringUtil::toString(j) + ";";
        }
        auto docs = test::DocumentCreator::CreateKVDocuments(_schema, docStr);
        SortDocumentContainer container;
        common::Locator locator;
        ASSERT_TRUE(container.init(_sortDescs, _schema, locator));
        for (size_t j = 0; j < docs.size(); j++) {
            container.pushDocument(docs[j]);
        }
        container.sortDocument();
        for (size_t j = 0; j < i; j++) {
            EXPECT_EQ((int64_t)j, container[j]->GetTimestamp());
        }
    }
}

TEST_F(SortDocumentContainerTest, testSortWithPk)
{
    {
        // 1. multi operate  with same pk
        SortDocumentContainer container;
        SetPKToSchema(_schema);
        _sortDescs[0].TEST_SetSortPattern(indexlibv2::config::sp_desc);
        common::Locator locator;
        ASSERT_TRUE(container.init(_sortDescs, _schema, locator));
        container.pushDocument(createDocument(0, "1", ADD_DOC, 0));
        container.pushDocument(createDocument(1, "1", UPDATE_FIELD, 0));
        container.pushDocument(createDocument(2, "1", ADD_DOC, 0));
        container.pushDocument(createDocument(3, "1", UPDATE_FIELD, 0));
        container.pushDocument(createDocument(4, "1", DELETE_DOC, 0));
        container.sortDocument();
        EXPECT_EQ(size_t(5), container.allDocCount());
        EXPECT_EQ(size_t(3), container.sortedDocCount());
        int64_t expectedTs[] = {2, 3, 4};
        for (size_t i = 0; i < container.sortedDocCount(); i++) {
            EXPECT_EQ(expectedTs[i], container[i]->GetTimestamp());
        }
    }
    {
        // 1. with equal sort field
        SortDocumentContainer container;
        SetPKToSchema(_schema);
        _sortDescs[0].TEST_SetSortPattern(indexlibv2::config::sp_desc);
        common::Locator locator;
        ASSERT_TRUE(container.init(_sortDescs, _schema, locator));
        container.pushDocument(createDocument(0, "0", ADD_DOC, 0));
        container.pushDocument(createDocument(1, "1", ADD_DOC, 1));
        container.pushDocument(createDocument(2, "2", ADD_DOC, 1));
        container.pushDocument(createDocument(3, "3", ADD_DOC, 2));
        container.sortDocument();
        EXPECT_EQ(size_t(4), container.allDocCount());
        EXPECT_EQ(size_t(4), container.sortedDocCount());
        EXPECT_EQ(int64_t(3), container[0]->GetTimestamp());
        EXPECT_EQ(int64_t(1), container[1]->GetTimestamp());
        EXPECT_EQ(int64_t(2), container[2]->GetTimestamp());
        EXPECT_EQ(int64_t(0), container[3]->GetTimestamp());
    }
}

TEST_F(SortDocumentContainerTest, testUnprocessedCount)
{
    SortDocumentContainer container;
    common::Locator locator;
    ASSERT_TRUE(container.init(_sortDescs, _schema, locator));
    EXPECT_EQ((uint32_t)0, container.getUnprocessedCount());
    container.pushDocument(createDocument(0, "", ADD_DOC));
    container.pushDocument(createDocument(1, "", ADD_DOC));
    container.sortDocument();
    EXPECT_EQ((uint32_t)2, container.getUnprocessedCount());
    container.clear();
    EXPECT_EQ((uint32_t)0, container.getUnprocessedCount());
}

TEST_F(SortDocumentContainerTest, testMergedLocator)
{
    SortDocumentContainer container;
    SetPKToSchema(_schema);
    _sortDescs[0].TEST_SetSortPattern(indexlibv2::config::sp_desc);
    common::Locator locator;
    ASSERT_TRUE(container.init(_sortDescs, _schema, locator));
    container.pushDocument(createDocument(0, "0", UPDATE_FIELD, 0, 0));
    container.pushDocument(createDocument(1, "0", UPDATE_FIELD, 1, 1));
    container.pushDocument(createDocument(2, "0", UPDATE_FIELD, 1, 2));
    container.pushDocument(createDocument(3, "1", ADD_DOC, 1, 3));
    container.pushDocument(createDocument(4, "2", ADD_DOC, 1, 4));
    container.sortDocument();
    EXPECT_EQ(int64_t(-1), container._firstLocator.GetOffset().first);
    EXPECT_EQ(int64_t(4), container._lastLocator.GetOffset().first);
    EXPECT_EQ(size_t(3), container.sortedDocCount());
    EXPECT_EQ(size_t(6), container.allDocCount());

    for (size_t i = 0; i < 2; i++) {
        ASSERT_TRUE(locator.Deserialize(container[i]->GetLocator().ToString()));
        ASSERT_EQ(int64_t(-1), locator.GetOffset().first);
    }
    ASSERT_TRUE(locator.Deserialize(container[2]->GetLocator().ToString()));
    ASSERT_EQ(int64_t(4), locator.GetOffset().first);
}

TEST_F(SortDocumentContainerTest, testLocator)
{
    SortDocumentContainer container;
    SetPKToSchema(_schema);
    _sortDescs[0].TEST_SetSortPattern(indexlibv2::config::sp_desc);
    common::Locator locator;
    ASSERT_TRUE(container.init(_sortDescs, _schema, locator));
    container.pushDocument(createDocument(0, "0", ADD_DOC, 0, 0));
    container.pushDocument(createDocument(1, "1", ADD_DOC, 1, 1));
    container.pushDocument(createDocument(2, "2", ADD_DOC, 1, 2));
    container.pushDocument(createDocument(3, "3", ADD_DOC, 2, 3));
    container.sortDocument();
    EXPECT_EQ(int64_t(-1), container._firstLocator.GetOffset().first);
    EXPECT_EQ(int64_t(3), container._lastLocator.GetOffset().first);
    EXPECT_EQ(size_t(4), container.sortedDocCount());
    EXPECT_EQ(size_t(4), container.allDocCount());

    for (size_t i = 0; i < 3; i++) {
        ASSERT_TRUE(locator.Deserialize(container[i]->GetLocator().ToString()));
        ASSERT_EQ(int64_t(-1), locator.GetOffset().first);
    }
    ASSERT_TRUE(locator.Deserialize(container[3]->GetLocator().ToString()));
    ASSERT_EQ(int64_t(3), locator.GetOffset().first);

    // swap
    SortDocumentContainer container2;
    container2.swap(container);
    EXPECT_EQ(int64_t(-1), container2._firstLocator.GetOffset().first);
    EXPECT_EQ(int64_t(3), container2._lastLocator.GetOffset().first);

    // clear
    container2.clear();
    ASSERT_EQ(int64_t(3), container2._firstLocator.GetOffset().first);
    ASSERT_EQ(int64_t(3), container2._lastLocator.GetOffset().first);
}

DocumentPtr newAttrDoc(const string& pk, const string& sortfield, const string& uint1, const string& uint2,
                       DocOperateType opType)
{
    return DocumentTestHelper::createDocument(FakeDocument::constructWithAttributes(
        pk,
        "field#uint32=" + sortfield + (uint1.empty() ? ";uint1" : ";uint1#uint32=" + uint1) +
            (uint2.empty() ? ";uint2" : ";uint2#uint32=" + uint2),
        opType));
}

TEST_F(SortDocumentContainerTest, testMergeUpdateDocWithSwap)
{
    SetPKToSchema(_schema);
    common::Locator locator;
    SortDocumentContainer container1;
    SortDocumentContainer container2;
    _sortDescs[0].TEST_SetSortPattern(indexlibv2::config::sp_desc);
    ASSERT_TRUE(container1.init(_sortDescs, _schema, locator));
    ASSERT_TRUE(container2.init(_sortDescs, _schema, locator));
    container2.swap(container1);

    container1.pushDocument(newAttrDoc("1", "0", "3", "3", UPDATE_FIELD));
    container1.pushDocument(newAttrDoc("1", "0", "1", "", UPDATE_FIELD));
    container1.pushDocument(newAttrDoc("1", "0", "", "2", UPDATE_FIELD));
    container1.sortDocument();

    container2.clear();
    container2.pushDocument(newAttrDoc("1", "0", "1", "", UPDATE_FIELD));
    container2.pushDocument(newAttrDoc("1", "0", "3", "3", UPDATE_FIELD));
    container2.pushDocument(newAttrDoc("1", "0", "", "2", UPDATE_FIELD));
    container2.sortDocument();

    uint32_t value0 = 0;
    StringView field0(reinterpret_cast<char*>(&value0), sizeof(value0));
    uint32_t value1 = 1;
    StringView field1(reinterpret_cast<char*>(&value1), sizeof(value1));
    uint32_t value2 = 2;
    StringView field2(reinterpret_cast<char*>(&value2), sizeof(value2));

    NormalDocumentPtr doc = DYNAMIC_POINTER_CAST(NormalDocument, container1[0]);
    EXPECT_EQ(field0, doc->GetAttributeDocument()->GetField(0));
    EXPECT_EQ(field1, doc->GetAttributeDocument()->GetField(1));
    EXPECT_EQ(field2, doc->GetAttributeDocument()->GetField(2));
}

TEST_F(SortDocumentContainerTest, testMergeUpdateDoc)
{
    SetPKToSchema(_schema);
    common::Locator locator;
    {
        // only update
        SortDocumentContainer container;
        _sortDescs[0].TEST_SetSortPattern(indexlibv2::config::sp_desc);
        ASSERT_TRUE(container.init(_sortDescs, _schema, locator));
        container.pushDocument(newAttrDoc("1", "0", "3", "3", UPDATE_FIELD));
        container.pushDocument(newAttrDoc("1", "0", "1", "", UPDATE_FIELD));
        container.pushDocument(newAttrDoc("1", "0", "", "2", UPDATE_FIELD));
        container.sortDocument();
        EXPECT_EQ(size_t(1), container.sortedDocCount());
        EXPECT_EQ(size_t(4), container.allDocCount());
        uint32_t value0 = 0;
        StringView field0(reinterpret_cast<char*>(&value0), sizeof(value0));
        uint32_t value1 = 1;
        StringView field1(reinterpret_cast<char*>(&value1), sizeof(value1));
        uint32_t value2 = 2;
        StringView field2(reinterpret_cast<char*>(&value2), sizeof(value2));

        NormalDocumentPtr doc = DYNAMIC_POINTER_CAST(NormalDocument, container[0]);
        EXPECT_EQ(field0, doc->GetAttributeDocument()->GetField(0));
        EXPECT_EQ(field1, doc->GetAttributeDocument()->GetField(1));
        EXPECT_EQ(field2, doc->GetAttributeDocument()->GetField(2));
    }
    {
        // update mix
        SortDocumentContainer container;
        _sortDescs[0].TEST_SetSortPattern(indexlibv2::config::sp_desc);
        ASSERT_TRUE(container.init(_sortDescs, _schema, locator));
        container.pushDocument(newAttrDoc("1", "0", "3", "3", ADD_DOC));
        container.pushDocument(newAttrDoc("1", "0", "3", "3", DELETE_DOC));

        container.pushDocument(newAttrDoc("1", "0", "3", "3", UPDATE_FIELD));
        container.pushDocument(newAttrDoc("1", "0", "1", "", UPDATE_FIELD));
        container.pushDocument(newAttrDoc("1", "0", "", "2", UPDATE_FIELD));

        container.pushDocument(newAttrDoc("1", "0", "3", "3", DELETE_DOC));

        container.pushDocument(newAttrDoc("1", "0", "6", "7", UPDATE_FIELD));
        container.pushDocument(newAttrDoc("1", "0", "4", "9", UPDATE_FIELD));
        container.pushDocument(newAttrDoc("1", "0", "8", "5", UPDATE_FIELD));

        container.pushDocument(newAttrDoc("1", "0", "3", "3", DELETE_DOC));
        container.pushDocument(newAttrDoc("1", "0", "3", "3", DELETE_SUB_DOC));

        container.sortDocument();
        EXPECT_EQ(size_t(7), container.sortedDocCount());
        EXPECT_EQ(size_t(13), container.allDocCount());
        EXPECT_EQ(ADD_DOC, container[0]->GetDocOperateType());
        EXPECT_EQ(DELETE_DOC, container[1]->GetDocOperateType());
        EXPECT_EQ(UPDATE_FIELD, container[2]->GetDocOperateType());
        EXPECT_EQ(DELETE_DOC, container[3]->GetDocOperateType());
        EXPECT_EQ(UPDATE_FIELD, container[4]->GetDocOperateType());
        EXPECT_EQ(DELETE_DOC, container[5]->GetDocOperateType());
        EXPECT_EQ(DELETE_SUB_DOC, container[6]->GetDocOperateType());
        uint32_t value0 = 0;
        StringView field0(reinterpret_cast<char*>(&value0), sizeof(value0));
        uint32_t value1 = 1;
        StringView field1(reinterpret_cast<char*>(&value1), sizeof(value1));
        uint32_t value2 = 2;
        StringView field2(reinterpret_cast<char*>(&value2), sizeof(value2));

        NormalDocumentPtr doc2 = DYNAMIC_POINTER_CAST(NormalDocument, container[2]);
        EXPECT_EQ(field0, doc2->GetAttributeDocument()->GetField(0));
        EXPECT_EQ(field1, doc2->GetAttributeDocument()->GetField(1));
        EXPECT_EQ(field2, doc2->GetAttributeDocument()->GetField(2));

        uint32_t value8 = 8;
        StringView field8(reinterpret_cast<char*>(&value8), sizeof(value8));
        uint32_t value5 = 5;
        StringView field5(reinterpret_cast<char*>(&value5), sizeof(value5));

        NormalDocumentPtr doc4 = DYNAMIC_POINTER_CAST(NormalDocument, container[4]);
        EXPECT_EQ(field0, doc4->GetAttributeDocument()->GetField(0));
        EXPECT_EQ(field8, doc4->GetAttributeDocument()->GetField(1));
        EXPECT_EQ(field5, doc4->GetAttributeDocument()->GetField(2));
    }
}

TEST_F(SortDocumentContainerTest, testMergeUpdateDocSubDoc)
{
    SetPKToSchema(_schema);
    common::Locator locator;
    {
        // sub doc do not merge
        SortDocumentContainer container;
        _schema->SetSubIndexPartitionSchema(IndexPartitionSchemaPtr(new IndexPartitionSchema()));
        _sortDescs[0].TEST_SetSortPattern(indexlibv2::config::sp_desc);
        ASSERT_TRUE(container.init(_sortDescs, _schema, locator));

        NormalDocumentPtr doc1 = DYNAMIC_POINTER_CAST(NormalDocument, newAttrDoc("1", "0", "3", "3", UPDATE_FIELD));
        NormalDocumentPtr subdoc1 = DYNAMIC_POINTER_CAST(NormalDocument, newAttrDoc("1", "0", "3", "3", UPDATE_FIELD));

        doc1->AddSubDocument(subdoc1);
        container.pushDocument(doc1);

        NormalDocumentPtr doc2 = DYNAMIC_POINTER_CAST(NormalDocument, newAttrDoc("1", "0", "3", "3", UPDATE_FIELD));
        NormalDocumentPtr subdoc2 = DYNAMIC_POINTER_CAST(NormalDocument, newAttrDoc("1", "0", "3", "3", UPDATE_FIELD));
        doc2->AddSubDocument(subdoc2);
        container.pushDocument(doc2);
        container.sortDocument();
        EXPECT_EQ(size_t(2), container.sortedDocCount());
        EXPECT_EQ(size_t(2), container.allDocCount());
    }
}

TEST_F(SortDocumentContainerTest, testMergeUpdateDocInvertedIndex)
{
    string fields = "pk:string;field:uint32;uint1:uint32;uint2:uint32";
    string indexes = "pk:primarykey64:pk;uint1:number:uint1";
    string attrs = "field;uint1;uint2";
    auto schema = test::SchemaMaker::MakeSchema(fields, indexes, attrs, /*summarys*/ "");
    auto indexConfig = schema->GetIndexSchema()->GetIndexConfig("uint1");
    indexConfig->TEST_SetIndexUpdatable(true);
    indexConfig->SetOptionFlag(0);

    common::Locator locator;
    {
        // inverted index update do not merge
        SortDocumentContainer container;
        _sortDescs[0].TEST_SetSortPattern(indexlibv2::config::sp_desc);
        ASSERT_TRUE(container.init(_sortDescs, schema, locator));

        NormalDocumentPtr doc1 = DYNAMIC_POINTER_CAST(NormalDocument, newAttrDoc("1", "0", "3", "3", UPDATE_FIELD));
        NormalDocumentPtr doc2 = DYNAMIC_POINTER_CAST(NormalDocument, newAttrDoc("1", "0", "3", "3", UPDATE_FIELD));

        container.pushDocument(doc1);
        container.pushDocument(doc2);

        container.sortDocument();
        EXPECT_EQ(size_t(2), container.sortedDocCount());
        EXPECT_EQ(size_t(2), container.allDocCount());
    }
}

DocumentPtr SortDocumentContainerTest::createDocument(docid_t docId, const string& pkStr, DocOperateType opType,
                                                      uint32_t sortField, int64_t locator, bool isNull)
{
    document::FakeDocument fakeDoc(pkStr, opType, true, true, docId, sortField, 0, locator, isNull);
    DocumentPtr doc = DocumentTestHelper::createDocument(fakeDoc);
    doc->SetTimestamp((int64_t)docId);
    return doc;
}

DocumentPtr SortDocumentContainerTest::createDocument(docid_t docId, const string& sortFieldInfo, const string& pkStr,
                                                      DocOperateType opType, int64_t locator)
{
    FakeDocument fakeDoc = FakeDocument::constructWithAttributes(pkStr, "field#" + sortFieldInfo, opType);
    fakeDoc._docId = docId;
    fakeDoc._locator = locator;
    DocumentPtr doc = DocumentTestHelper::createDocument(fakeDoc);
    doc->SetTimestamp((int64_t)docId);
    return doc;
}

}} // namespace build_service::builder
