#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include <algorithm>
#include "indexlib/index/normal/attribute/accessor/section_attribute_writer.h"
#include "indexlib/util/numeric_util.h"
#include "indexlib/index/test/index_test_util.h"
#include "indexlib/index/normal/attribute/test/attribute_test_util.h"
#include <autil/mem_pool/SimpleAllocator.h>
#include "fslib/fslib.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/index/normal/attribute/test/attribute_writer_helper.h"
#include "indexlib/file_system/indexlib_file_system_impl.h"
#include "indexlib/file_system/in_mem_directory.h"
#include "indexlib/document/document_rewriter/section_attribute_rewriter.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"

IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(index);

using namespace std;
using namespace autil::legacy;
using namespace autil::mem_pool;
using namespace fslib;
using namespace fslib::fs;

class SectionAttributeWriterTest : public INDEXLIB_TESTBASE
{
public:
    string mDir;

    void CaseSetUp() override
    {
        mDir = GET_TEST_DATA_PATH() + "/";
    }

    void CaseTearDown() override
    {
    }

    void TestWriter()
    {
        IndexPartitionSchemaPtr schema =
            AttributeTestUtil::MakeSchemaWithPackageIndexConfig("test_pack", 32);
        PackageIndexConfigPtr packIndexConfig =
            DYNAMIC_POINTER_CAST(PackageIndexConfig,
                    schema->GetIndexSchema()->GetIndexConfig("test_pack"));
        SectionAttributeWriter writer(packIndexConfig);
        writer.Init(NULL);

        uint32_t docNum = 30;
        BuildData(schema, writer, docNum);
        
        CheckData(packIndexConfig, docNum);
    }

 private:
    
    void BuildData(const IndexPartitionSchemaPtr& schema,
		   SectionAttributeWriter& writer, uint32_t docNum)
    {
        IndexTestUtil::ResetDir(mDir);
        string indexDir = FileSystemWrapper::JoinPath(mDir, "segment/index/");
        FileSystemWrapper::MkDir(indexDir, true);

	SectionAttributeRewriter rewriter;
	rewriter.Init(schema);

        for(docid_t i = 0; i < (docid_t)docNum; ++i)
        {
	    NormalDocumentPtr doc(new NormalDocument);
            autil::mem_pool::Pool* pool = doc->GetPool();
	    IndexDocumentPtr indexDocument(new IndexDocument(pool));
	    doc->SetIndexDocument(indexDocument);
	    doc->ModifyDocOperateType(ADD_DOC);

            uint32_t sectionPerDoc = i % MAX_SECTION_COUNT_PER_DOC + 1;
            for (uint32_t j = 0; j < sectionPerDoc; ++j)
            {
                IndexTokenizeField* field = IE_POOL_COMPATIBLE_NEW_CLASS(pool, IndexTokenizeField, pool);
                field->SetFieldId((fieldid_t)((i + j + 1) % 32));
                
                Section* section = IE_POOL_COMPATIBLE_NEW_CLASS(pool, Section, 8, pool);
                section->SetLength((section_len_t)((i + j + 2) % 2047 + 1));
                section->SetWeight((section_weight_t)((i+j) % 65536));
                
                field->AddSection(section);
		indexDocument->SetField(field->GetFieldId(), field);
            }

	    doc->SetDocId((docid_t)i);
	    rewriter.Rewrite(doc);
	    writer.EndDocument(*indexDocument);
        }

        FileSystemOptions fileSystemOptions;
        fileSystemOptions.needFlush = true;
        IndexlibFileSystemPtr fileSystem(
                new IndexlibFileSystemImpl(indexDir));
        util::MemoryQuotaControllerPtr quotaControllor(
                new util::MemoryQuotaController(1024*1024*1024));
        util::PartitionMemoryQuotaControllerPtr controllor(
                new util::PartitionMemoryQuotaController(quotaControllor));

        fileSystemOptions.memoryQuotaController = controllor;
        fileSystem->Init(fileSystemOptions);
        DirectoryPtr dir(new InMemDirectory(indexDir, fileSystem));
        SimplePool simplePool;
        writer.Dump(dir, &simplePool);
        fileSystem->Sync(true);
    }
    
    void CheckData(const PackageIndexConfigPtr& indexConfig, 
                   uint32_t docNum)
    {
        string indexDir = FileSystemWrapper::JoinPath(mDir, "segment/index/");
        string sectionPath = indexDir + indexConfig->GetIndexName() + "_section/";
        string offsetFilePath = sectionPath + ATTRIBUTE_OFFSET_FILE_NAME;

        FileMeta meta;
        ErrorCode ret = FileSystem::getFileMeta(offsetFilePath, meta);
        INDEXLIB_TEST_EQUAL(EC_OK, ret);
        INDEXLIB_TEST_EQUAL(sizeof(uint32_t) * (docNum + 1), (size_t)meta.fileLength);

        string dataFilePath = sectionPath +  ATTRIBUTE_DATA_FILE_NAME;
        ret = FileSystem::getFileMeta(dataFilePath, meta);
        INDEXLIB_TEST_EQUAL(EC_OK, ret);
    }
};

INDEXLIB_UNIT_TEST_CASE(SectionAttributeWriterTest, TestWriter);

IE_NAMESPACE_END(index);

