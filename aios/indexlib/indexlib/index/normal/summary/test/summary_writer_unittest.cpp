#include <autil/StringUtil.h>
#include <autil/mem_pool/Pool.h>
#include <autil/ZlibCompressor.h>
#include "fslib/fs/FileSystem.h"
#include "fslib/fs/File.h"
#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/summary/summary_writer_impl.h"
#include "indexlib/index/normal/summary/local_disk_summary_writer.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index/test/partition_schema_maker.h"
#include "indexlib/index/test/index_test_util.h"
#include "indexlib/document/index_document/normal_document/summary_formatter.h"
#include "indexlib/util/path_util.h"

using namespace std;
using namespace std::tr1;
using namespace autil::legacy;
using namespace autil;
using namespace fslib;
using namespace fslib::fs;

IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(common);

IE_NAMESPACE_BEGIN(index);

class SummaryWriterTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(SummaryWriterTest);

public:
    void CaseSetUp() override
    {
        mDir = GET_TEST_DATA_PATH() + "/";
    }

    void CaseTearDown() override
    {
    }

    void TestCaseForWriteWithOutCompress()
    {
        TestCaseForWrite(false);
    }
    
    void TestCaseForWriteWithCompress()
    {
        TestCaseForWrite(true);
    }

    void TestCaseForUpdateBuildMetrics()
    {
        IndexPartitionSchemaPtr schema(new IndexPartitionSchema);
        PartitionSchemaMaker::MakeSchema(schema,
                "title:text;user_name:string;user_id:integer;price:float", 
                "", "", "title;user_name;price");

        BuildResourceMetrics buildResourceMetrics;
        buildResourceMetrics.Init();
        LocalDiskSummaryWriterPtr writer(new LocalDiskSummaryWriter());
        writer->Init(schema->GetSummarySchema()->GetSummaryGroupConfig(DEFAULT_SUMMARYGROUPID),
                     &buildResourceMetrics);

        int64_t allocateSize = writer->mPool->getUsedBytes();
        int64_t curMemoryUse = buildResourceMetrics.GetValue(BMT_CURRENT_MEMORY_USE);
        ASSERT_EQ(allocateSize, curMemoryUse);

        // test allocate & update build metrics
        writer->mPool->allocate(30);
        writer->UpdateBuildResourceMetrics();
        allocateSize = writer->mPool->getUsedBytes();
        curMemoryUse = buildResourceMetrics.GetValue(BMT_CURRENT_MEMORY_USE);
        ASSERT_EQ(allocateSize, curMemoryUse);

        // test allocate & update build metrics over 2G
        writer->mPool->allocate(1024 * 1024 * 1024);
        writer->mPool->allocate(1024 * 1024 * 1024);
        writer->UpdateBuildResourceMetrics();
        allocateSize = writer->mPool->getUsedBytes();
        curMemoryUse = buildResourceMetrics.GetValue(BMT_CURRENT_MEMORY_USE);
        ASSERT_EQ(allocateSize, curMemoryUse);
    }
    
private:
    void TestCaseForWrite(bool useCompress)
    {
        const uint32_t documentCount = 10;
        IndexPartitionSchemaPtr schema;
        vector<string> expectedSummaryVector;

        SummaryWriterPtr summaryWriter = CreateSummaryWriter(useCompress, schema);
        vector<SummaryDocumentPtr> summaryDocs;
        autil::mem_pool::Pool pool;
        MakeDocuments(documentCount, schema, summaryDocs, expectedSummaryVector, &pool, useCompress);
        for (uint32_t i = 0; i< (uint32_t)documentCount; ++i)
        {   
            SerializedSummaryDocumentPtr serDoc;
            SummaryFormatter formatter(schema->GetSummarySchema());
            formatter.SerializeSummaryDoc(summaryDocs[i], serDoc);
            summaryWriter->AddDocument(serDoc);
        }

        file_system::DirectoryPtr directory = GET_PARTITION_DIRECTORY();
        summaryWriter->Dump(directory);
        string dir = directory->GetPath();

        string offsetPath = PathUtil::JoinPath(dir, SUMMARY_OFFSET_FILE_NAME);
        string dataPath = PathUtil::JoinPath(dir, SUMMARY_DATA_FILE_NAME);

        FileMeta offsetFileMeta;
        fslib::ErrorCode err = fs::FileSystem::getFileMeta(offsetPath, offsetFileMeta);
        INDEXLIB_TEST_EQUAL(EC_OK, err);
        INDEXLIB_TEST_EQUAL(documentCount * sizeof(uint64_t), (uint64_t)offsetFileMeta.fileLength);

        unique_ptr<fs::File> offsetFile(fs::FileSystem::openFile(offsetPath, READ));
        uint64_t lastOffset;
        offsetFile->pread((void*)(&lastOffset), sizeof(uint64_t), 
                         (documentCount - 1) * sizeof(uint64_t));

        FileMeta dataFileMeta;
        err = fs::FileSystem::getFileMeta(dataPath, dataFileMeta);
        INDEXLIB_TEST_EQUAL(EC_OK, err);
        INDEXLIB_TEST_TRUE(dataFileMeta.fileLength > (int64_t)lastOffset);
    }

    SummaryWriterPtr CreateSummaryWriter(bool useCompress, IndexPartitionSchemaPtr& schema)
    {
        schema.reset(new IndexPartitionSchema);
        PartitionSchemaMaker::MakeSchema(schema,
                "title:text;user_name:string;user_id:integer;price:float", 
                "", "", "title;user_name;price");
        schema->GetRegionSchema(DEFAULT_REGIONID)->SetSummaryCompress(useCompress);

        SummaryWriterPtr summaryWriter(new SummaryWriterImpl());
        summaryWriter->Init(schema->GetSummarySchema(), NULL);
        return summaryWriter;
    }

    void MakeDocuments(uint32_t total, 
                       IndexPartitionSchemaPtr schema,
                       vector<SummaryDocumentPtr>& summaryDocs, 
                       vector<string>& expectedSummaryVector,
                       autil::mem_pool::Pool *pool,
                       bool useCompress)
    {
        for (uint32_t i = 0; i< total; ++i)
        {        
            docid_t docId = (docid_t)i;
            int32_t beginContentId = i;
            string expectedSummaryString;

            SummaryDocumentPtr doc(MakeOneSummaryDocument(docId, beginContentId, 
                            pool, schema.get(), expectedSummaryString, useCompress));
            expectedSummaryVector.push_back(expectedSummaryString);
            summaryDocs.push_back(doc);
        }
    }

    string MakeDocKey(docid_t docId)
    {
        string strId;
        autil::StringUtil::serializeUInt64((uint64_t)docId, strId);
        return strId;
    }

    SummaryDocumentPtr MakeOneSummaryDocument(docid_t docId, int beginContentId,
            autil::mem_pool::Pool *pool,IndexPartitionSchema* schema,
            string& expectedSummaryString, bool useCompress = false)
    {
        SummaryDocumentPtr doc(new SummaryDocument());
        doc->SetDocId(docId);

        expectedSummaryString.clear();

        FieldSchemaPtr fieldSchema = schema->GetFieldSchema();
        for (size_t i = 0; i < fieldSchema->GetFieldCount(); ++i)
        {
            FieldConfigPtr fieldConfig = fieldSchema->GetFieldConfig((fieldid_t)i);
            string content = "content_" + Int32ToStr(i + beginContentId);

            if (schema->GetSummarySchema()->IsInSummary((fieldid_t)i))
            { 
                doc->SetField(fieldConfig->GetFieldId(), autil::ConstString(content, pool));

                fieldid_t fieldId = fieldConfig->GetFieldId();
                uint32_t contentSize = (uint32_t)content.size();
                if (contentSize > 0)
                {
                    expectedSummaryString.append((const char*)&fieldId, 
                            sizeof(fieldid_t));
                    expectedSummaryString.append((const char*)&contentSize, sizeof(contentSize));
                    expectedSummaryString.append(content);
                }
            }
        }

        if(useCompress)
        {
            ZlibCompressor compressor;
            compressor.addDataToBufferIn(expectedSummaryString.c_str(),
                    expectedSummaryString.length());
            compressor.compress();
            expectedSummaryString.clear();

            expectedSummaryString.append(compressor.getBufferOut(), compressor.getBufferOutLen());
        }
        return doc;
    }

    string Int32ToStr(int32_t value)
    {
        stringstream stream;
        stream << value;
        return stream.str();
    }

private:
    util::BuildResourceMetrics mBuildResourceMetrics;
    string mDir;
};

INDEXLIB_UNIT_TEST_CASE(SummaryWriterTest, TestCaseForWriteWithOutCompress);
INDEXLIB_UNIT_TEST_CASE(SummaryWriterTest, TestCaseForWriteWithCompress);
INDEXLIB_UNIT_TEST_CASE(SummaryWriterTest, TestCaseForUpdateBuildMetrics);


IE_NAMESPACE_END(index);

