#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/inverted_index/accessor/index_accessory_reader.h"
#include "indexlib/index/normal/attribute/accessor/section_attribute_reader_impl.h"
#include "indexlib/index/test/partition_schema_maker.h"
#include "indexlib/index/test/index_test_util.h"
#include "indexlib/config/impl/index_partition_schema_impl.h"
#include "indexlib/config/impl/region_schema_impl.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);

class SectionAttributeReaderImplMock : public SectionAttributeReaderImpl
{
public:
    SectionAttributeReaderImplMock() {}
    SectionAttributeReaderImplMock(const SectionAttributeReaderImplMock& other)
    {}

    ~SectionAttributeReaderImplMock() {}

public:
    virtual void Open(const config::PackageIndexConfigPtr& indexConfig,
                      const index_base::PartitionDataPtr& partitionData)
    {}

    SectionAttributeReader* Clone() const
    {
        return new SectionAttributeReaderImplMock(*this);
    }
};

class IndexAccessoryReaderMock : public IndexAccessoryReader
{
public:
    IndexAccessoryReaderMock(const IndexSchemaPtr& schema)
        : IndexAccessoryReader(schema)
    {}

    IndexAccessoryReaderMock(const IndexAccessoryReaderMock& other)
        : IndexAccessoryReader((const IndexAccessoryReader&)other)
    {}

    ~IndexAccessoryReaderMock() {}

    IndexAccessoryReaderMock* Clone() const 
    {
        return new IndexAccessoryReaderMock(*this);
    }

private:
    
    virtual SectionAttributeReaderImplPtr OpenSectionReader(
            const config::PackageIndexConfigPtr& indexConfig,
            const index_base::PartitionDataPtr& partitionData) override
    {
        SectionAttributeReaderImplPtr sectionReader(
                new SectionAttributeReaderImplMock());
        return sectionReader;
    }
};

class IndexAccessoryReaderTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(IndexAccessoryReaderTest);
    void CaseSetUp() override
    {
        mRootDir = GET_TEST_DATA_PATH();
    }

    void CaseTearDown() override
    {
    }

    void TestCaseForOpen()
    {
        Version version(1);
        PartitionDataPtr partitionData = 
            IndexTestUtil::CreatePartitionData(GET_FILE_SYSTEM(), version);
        CreateDefaultSchema();
        IndexAccessoryReaderPtr accessoryReader(
                new IndexAccessoryReaderMock(mSchema->GetIndexSchema()));
        INDEXLIB_TEST_TRUE(accessoryReader->Open(partitionData));
        INDEXLIB_TEST_EQUAL(accessoryReader->mSectionReaderMap.size(), (size_t)2);
        INDEXLIB_TEST_EQUAL(accessoryReader->mSectionReaderVec.size(), (size_t)1);

        // no valid index accessory
        mSchema.reset(new IndexPartitionSchema);
        PartitionSchemaMaker::MakeSchema(mSchema,
                "text1:text;text2:text;string1:string;long1:long", // above is field schema
                "pack1:pack:text1,text2;pk:primarykey64:string1;", // above is index schema
                "string1;long1", // above is attribute schema
                "string1");// above is summary schema

        IndexConfigPtr indexConfig = mSchema->GetIndexSchema()->GetIndexConfig("pack1");
        PackageIndexConfigPtr packIndexConfig = 
                std::tr1::dynamic_pointer_cast<config::PackageIndexConfig>(indexConfig);
        assert(packIndexConfig != NULL);
        packIndexConfig->SetHasSectionAttributeFlag(false);

        AddTruncIndexConfig("pack1", "truncate_pack1");
        accessoryReader.reset(
                new IndexAccessoryReaderMock(mSchema->GetIndexSchema()));
        //TODO: now we return true
        ASSERT_TRUE(accessoryReader->Open(partitionData));
        INDEXLIB_TEST_EQUAL(accessoryReader->mSectionReaderMap.size(), (size_t)0);
        INDEXLIB_TEST_EQUAL(accessoryReader->mSectionReaderVec.size(), (size_t)0);
    }

    void TestCaseForClone()
    {
        Version version(1);
        PartitionDataPtr partitionData = 
            IndexTestUtil::CreatePartitionData(GET_FILE_SYSTEM(), version);

        CreateDefaultSchema();
        IndexAccessoryReaderPtr accessoryReader(
                new IndexAccessoryReaderMock(mSchema->GetIndexSchema()));
        INDEXLIB_TEST_TRUE(accessoryReader->Open(partitionData));

        IndexAccessoryReaderPtr cloneReader(accessoryReader->Clone());
        INDEXLIB_TEST_TRUE(cloneReader != NULL);
        INDEXLIB_TEST_TRUE(accessoryReader->mSectionReaderVec != cloneReader->mSectionReaderVec);
        INDEXLIB_TEST_TRUE(accessoryReader->mSectionReaderMap == cloneReader->mSectionReaderMap);
    }

    void TestCaseForGetSectionReader()
    {
        Version version(1);
        PartitionDataPtr partitionData = 
            IndexTestUtil::CreatePartitionData(GET_FILE_SYSTEM(), version);

        CreateDefaultSchema();
        IndexAccessoryReaderPtr accessoryReader(
                new IndexAccessoryReaderMock(mSchema->GetIndexSchema()));
        INDEXLIB_TEST_TRUE(accessoryReader->Open(partitionData));
        INDEXLIB_TEST_TRUE(accessoryReader->GetSectionReader("pack1") != NULL);
        INDEXLIB_TEST_TRUE(accessoryReader->GetSectionReader("pk") == NULL);
        INDEXLIB_TEST_EQUAL(accessoryReader->GetSectionReader("pack1"), 
                accessoryReader->GetSectionReader("truncate_pack1"));
    }
    
    void TestCaseForDelete()
    {
        Version version(1);
        PartitionDataPtr partitionData = 
            IndexTestUtil::CreatePartitionData(GET_FILE_SYSTEM(), version);

        IndexPartitionSchemaPtr schema(new IndexPartitionSchema);
        PartitionSchemaMaker::MakeSchema(schema,
                "text1:text;text2:text;string1:string;long1:long", // above is field schema
                "pack1:pack:text1,text2;pk:primarykey64:string1;", // above is index schema
                "string1;long1", // above is attribute schema
                "string1");// above is summary schema
        {
            IndexAccessoryReaderPtr accessoryReader(
                new IndexAccessoryReaderMock(schema->GetIndexSchema()));
            INDEXLIB_TEST_TRUE(accessoryReader->Open(partitionData));
            INDEXLIB_TEST_TRUE(accessoryReader->GetSectionReader("pack1") != NULL);
        }
        {
            // TODO test delete pack index
            schema->GetIndexSchema()->DeleteIndex("pack1");
            IndexAccessoryReaderPtr accessoryReader(
                new IndexAccessoryReaderMock(schema->GetIndexSchema()));
            INDEXLIB_TEST_TRUE(accessoryReader->Open(partitionData));
            INDEXLIB_TEST_TRUE(accessoryReader->GetSectionReader("pack1") == NULL);
        }
    }

    void TestCaseForDisable()
    {
        Version version(1);
        PartitionDataPtr partitionData = 
            IndexTestUtil::CreatePartitionData(GET_FILE_SYSTEM(), version);

        IndexPartitionSchemaPtr schema(new IndexPartitionSchema);
        PartitionSchemaMaker::MakeSchema(schema,
                "text1:text;text2:text;string1:string;long1:long", // above is field schema
                "pack1:pack:text1,text2;pk:primarykey64:string1", // above is index schema
                "string1;long1", // above is attribute schema
                "string1");// above is summary schema
        // TODO test disable pack index
        {
            IndexAccessoryReaderPtr accessoryReader(
                new IndexAccessoryReaderMock(schema->GetIndexSchema()));
            INDEXLIB_TEST_TRUE(accessoryReader->Open(partitionData));
            INDEXLIB_TEST_TRUE(accessoryReader->GetSectionReader("pack1") != NULL);
        }
        {
            schema->GetIndexSchema()->DisableIndex("pack1");
            IndexAccessoryReaderPtr accessoryReader(
                new IndexAccessoryReaderMock(schema->GetIndexSchema()));
            INDEXLIB_TEST_TRUE(accessoryReader->Open(partitionData));
            INDEXLIB_TEST_TRUE(accessoryReader->GetSectionReader("pack1") == NULL);
        }
    }


private:
    void AddTruncIndexConfig(const std::string& indexName,
                             const std::string& truncIndexName)
    {
        assert(mSchema != NULL);
        AddTruncIndexConfig(mSchema, indexName, truncIndexName);
    }
    void AddTruncIndexConfig(config::IndexPartitionSchemaPtr& schema,
                             const std::string& indexName,
                             const std::string& truncIndexName)
    {
        IndexSchemaPtr indexSchema = schema->GetIndexSchema();
        IndexConfigPtr indexConfig = indexSchema->GetIndexConfig(indexName);
        assert(indexConfig != NULL);

        IndexConfigPtr truncIndexConfig = IndexConfigPtr(indexConfig->Clone());
        assert(truncIndexConfig != NULL);

        truncIndexConfig->SetIndexName(truncIndexName);
        truncIndexConfig->SetVirtual(true);
        truncIndexConfig->SetNonTruncateIndexName(indexName);
        indexSchema->AddIndexConfig(truncIndexConfig);
    }

    void CreateDefaultSchema()
    {
        mSchema.reset(new IndexPartitionSchema);
        PartitionSchemaMaker::MakeSchema(mSchema,
                "text1:text;text2:text;string1:string;long1:long", // above is field schema
                "pack1:pack:text1,text2;pk:primarykey64:string1;", // above is index schema
                "string1;long1", // above is attribute schema
                "string1");// above is summary schema

        AddTruncIndexConfig(mSchema, "pack1", "truncate_pack1");
    }


private:
    string mRootDir;
    IndexPartitionSchemaPtr mSchema;
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(index, IndexAccessoryReaderTest);

INDEXLIB_UNIT_TEST_CASE(IndexAccessoryReaderTest, TestCaseForOpen);
INDEXLIB_UNIT_TEST_CASE(IndexAccessoryReaderTest, TestCaseForClone);
INDEXLIB_UNIT_TEST_CASE(IndexAccessoryReaderTest, TestCaseForGetSectionReader);
INDEXLIB_UNIT_TEST_CASE(IndexAccessoryReaderTest, TestCaseForDelete);
INDEXLIB_UNIT_TEST_CASE(IndexAccessoryReaderTest, TestCaseForDisable);

IE_NAMESPACE_END(index);
