#ifndef  SECTION_ATTRIBUTE_READER_TEST
#define  SECTION_ATTRIBUTE_READER_TEST
#endif

#include <tr1/memory>
#include <autil/mem_pool/SimpleAllocator.h>
#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/numeric_util.h"
#include "indexlib/index/normal/attribute/accessor/section_attribute_writer.h"
#include "indexlib/index/normal/attribute/accessor/section_attribute_reader_impl.h"
#include "indexlib/index/normal/attribute/in_doc_multi_section_meta.h"
#include "indexlib/index/normal/attribute/test/attribute_test_util.h"
#include "indexlib/index/normal/attribute/test/section_attribute_test_util.h"
#include "indexlib/index/test/index_test_util.h"
#include "fslib/fslib.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/index_base/partition_data.h"

using namespace std;
using namespace std::tr1;
using namespace autil::legacy;
using namespace autil::mem_pool;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);

class SectionAttributeReaderTest : public INDEXLIB_TESTBASE
{
public:
    typedef map<docid_t, vector<SectionMeta> > Answer;
    DECLARE_CLASS_NAME(SectionAttributeReaderTest);

public:
    void CaseSetUp() override
    {
        mIndexName = "test_pack";
    }

    void CaseTearDown() override
    {
    }

    void TestOpenForOneSegment()
    {
        // todo: big field id
        vector<uint32_t> fullBuildDocCounts;
        fullBuildDocCounts.push_back(10);

        InnerTestOpen(fullBuildDocCounts);
    }

    void TestOpenForMultiSegments()
    {
        // todo: big field id
        vector<uint32_t> fullBuildDocCounts;
        fullBuildDocCounts.push_back(30);
        fullBuildDocCounts.push_back(100);
        fullBuildDocCounts.push_back(80);
        fullBuildDocCounts.push_back(0);
        fullBuildDocCounts.push_back(50);

        InnerTestOpen(fullBuildDocCounts);
    }

    void TestReOpen()
    {
        //TODO: reader use open to reopen index
	IndexPartitionSchemaPtr schema =
	    AttributeTestUtil::MakeSchemaWithPackageIndexConfig(mIndexName, MAX_FIELD_COUNT);
        PackageIndexConfigPtr packIndexConfig =
	    DYNAMIC_POINTER_CAST(PackageIndexConfig,
				 schema->GetIndexSchema()->GetIndexConfig(mIndexName));

        vector<uint32_t> fullBuildDocCounts;
        fullBuildDocCounts.push_back(100);
        fullBuildDocCounts.push_back(230);
        fullBuildDocCounts.push_back(30);

        Answer answer;
        FullBuild(schema, fullBuildDocCounts, answer);

        vector<uint32_t> incBuildDocCounts;
        incBuildDocCounts.push_back(20);
        incBuildDocCounts.push_back(40);

        for (size_t i = 0; i < incBuildDocCounts.size(); ++i)
        {
            segmentid_t segId = fullBuildDocCounts.size() + i;
            IncBuild(schema, segId, incBuildDocCounts[i], answer);

            SectionAttributeReaderImplPtr reader(new SectionAttributeReaderImpl());
            PartitionDataPtr partitionData = IndexTestUtil::CreatePartitionData(
                    GET_FILE_SYSTEM(), fullBuildDocCounts.size() + i + 1);
            reader->Open(packIndexConfig, partitionData);
            CheckData(*reader, answer);
        }
    }

    void TestRead()
    {
        Answer answer;
	IndexPartitionSchemaPtr schema =
	    AttributeTestUtil::MakeSchemaWithPackageIndexConfig(mIndexName, MAX_FIELD_COUNT);
        PackageIndexConfigPtr packIndexConfig =
	    DYNAMIC_POINTER_CAST(PackageIndexConfig,
				 schema->GetIndexSchema()->GetIndexConfig(mIndexName));

        vector<uint32_t> fullBuildDocCounts;
        fullBuildDocCounts.push_back(1);
        FullBuild(schema, fullBuildDocCounts, answer);
        
        SectionAttributeReaderImpl reader;
        PartitionDataPtr partitionData = IndexTestUtil::CreatePartitionData(
                GET_FILE_SYSTEM(), fullBuildDocCounts.size());
        reader.Open(packIndexConfig, partitionData);

        size_t bufLen  = (sizeof(section_len_t)
                          + sizeof(section_weight_t) 
                          + sizeof(section_fid_t))
                         * MAX_SECTION_COUNT_PER_DOC;
        uint8_t buf[bufLen];
        INDEXLIB_TEST_EQUAL(0, reader.Read(0, buf, bufLen));
        INDEXLIB_TEST_EQUAL(-1, reader.Read(1, buf, bufLen));
    }

    void TestGetSection()
    {
        Answer answer;
	
	IndexPartitionSchemaPtr schema =
	    AttributeTestUtil::MakeSchemaWithPackageIndexConfig(mIndexName, MAX_FIELD_COUNT);
        PackageIndexConfigPtr packIndexConfig =
	    DYNAMIC_POINTER_CAST(PackageIndexConfig,
				 schema->GetIndexSchema()->GetIndexConfig(mIndexName));

        vector<uint32_t> fullBuildDocCounts;
        fullBuildDocCounts.push_back(1);
        FullBuild(schema, fullBuildDocCounts, answer);
        
        SectionAttributeReaderImpl reader;
        PartitionDataPtr partitionData = IndexTestUtil::CreatePartitionData(
                GET_FILE_SYSTEM(), fullBuildDocCounts.size());
        reader.Open(packIndexConfig, partitionData);

        INDEXLIB_TEST_TRUE(reader.GetSection(0) != NULL);
        INDEXLIB_TEST_TRUE(reader.GetSection(1) == NULL);
    }

private:
    void InnerTestOpen(const vector<uint32_t>& fullBuildDocCounts)
    {
        Answer answer;
	IndexPartitionSchemaPtr schema =
	    AttributeTestUtil::MakeSchemaWithPackageIndexConfig(mIndexName, MAX_FIELD_COUNT);
	
	PackageIndexConfigPtr packIndexConfig =
	    DYNAMIC_POINTER_CAST(PackageIndexConfig,
				 schema->GetIndexSchema()->GetIndexConfig(mIndexName));
        FullBuild(schema, fullBuildDocCounts, answer);
        
        SectionAttributeReaderImpl reader;
        PartitionDataPtr partitionData = IndexTestUtil::CreatePartitionData(
                GET_FILE_SYSTEM(), fullBuildDocCounts.size());
        reader.Open(packIndexConfig, partitionData);
        CheckData(reader, answer);
    }

    void FullBuild(const IndexPartitionSchemaPtr& schema,
                   const vector<uint32_t>& fullBuildDocCounts, 
                   Answer& answer)
    {
        TearDown();
        SetUp();
        SectionAttributeTestUtil::BuildMultiSegmentsData(
                GET_PARTITION_DIRECTORY(),
		schema, mIndexName,
                fullBuildDocCounts, answer);
    }

    void IncBuild(const IndexPartitionSchemaPtr& schema,
                  segmentid_t segId,
                  uint32_t docCount,
                  Answer& answer)
    {
        SectionAttributeTestUtil::BuildOneSegmentData(GET_PARTITION_DIRECTORY(), 
						      segId, schema, mIndexName,
						      docCount, answer);
    }

    void CheckData(const SectionAttributeReaderImpl& reader, Answer& answer)
    {
        size_t sizePerSection = sizeof(section_len_t) + sizeof(section_weight_t) 
                                + sizeof(section_fid_t);

        for (size_t i = 0; i < answer.size(); ++i) 
        {
            const vector<SectionMeta> &expectedMeta = answer[i];

            uint8_t buf[sizePerSection * MAX_SECTION_COUNT_PER_DOC];
            uint32_t ret = reader.Read((docid_t)i, buf,
                                       sizeof(uint8_t) * sizePerSection * MAX_SECTION_COUNT_PER_DOC);
            assert(0 == ret);
            INDEXLIB_TEST_EQUAL((uint32_t)0, ret);

            MultiSectionMeta meta;
            meta.Init(buf, reader.HasFieldId(), reader.HasSectionWeight());
            CheckSectionMetaInOneDoc(expectedMeta, meta);

            InDocSectionMetaPtr inDocSectionMeta = reader.GetSection((docid_t)i);
            InDocMultiSectionMetaPtr inDocMultiSectionMeta = 
                dynamic_pointer_cast<InDocMultiSectionMeta>(inDocSectionMeta);
            INDEXLIB_TEST_TRUE(inDocMultiSectionMeta != NULL);
            CheckSectionMetaInOneDoc(expectedMeta, *inDocMultiSectionMeta);            
        }
    }

    void CheckSectionMetaInOneDoc(const vector<SectionMeta>& expectedMeta, 
                                  const MultiSectionMeta& meta) 
    {
        uint32_t sectionCount = meta.GetSectionCount();
        INDEXLIB_TEST_EQUAL(expectedMeta.size(), sectionCount);
        
        for (size_t i = 0; i < sectionCount; ++i)
        {
            section_fid_t fieldId = meta.GetFieldId(i);
            INDEXLIB_TEST_EQUAL(expectedMeta[i].fieldId, fieldId);

            section_len_t sectionLen = meta.GetSectionLen(i);
            INDEXLIB_TEST_EQUAL(expectedMeta[i].length, sectionLen);

            section_weight_t sectionWeight = meta.GetSectionWeight(i);
            INDEXLIB_TEST_EQUAL(expectedMeta[i].weight, sectionWeight);   
        }
    }

private:
    static const uint32_t MAX_FIELD_COUNT = 32;
    string mIndexName;
};

INDEXLIB_UNIT_TEST_CASE(SectionAttributeReaderTest, TestRead);
INDEXLIB_UNIT_TEST_CASE(SectionAttributeReaderTest, TestGetSection);
INDEXLIB_UNIT_TEST_CASE(SectionAttributeReaderTest, TestOpenForOneSegment);
INDEXLIB_UNIT_TEST_CASE(SectionAttributeReaderTest, TestOpenForMultiSegments);
INDEXLIB_UNIT_TEST_CASE(SectionAttributeReaderTest, TestReOpen);

IE_NAMESPACE_END(index);
