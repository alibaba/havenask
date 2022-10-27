#include "indexlib/partition/test/index_builder_exception_unittest.h"
#include "indexlib/partition/offline_partition_writer.h"
#include "indexlib/partition/offline_partition.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index/test/partition_schema_maker.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/test/partition_data_maker.h"
#include "indexlib/document/document.h"
#include "indexlib/test/document_creator.h"
#include "indexlib/util/path_util.h"

using namespace std;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, IndexBuilderExceptionTest);

IndexBuilderExceptionTest::IndexBuilderExceptionTest()
{
}

IndexBuilderExceptionTest::~IndexBuilderExceptionTest()
{
}

void IndexBuilderExceptionTest::CaseSetUp()
{
    mRootDir = GET_TEST_DATA_PATH();
    mSchema.reset(new IndexPartitionSchema);
    PartitionSchemaMaker::MakeSchema(mSchema,
            //Field schema
            "string0:string;string1:string;long1:uint32;string2:string:true", 
            //Index schema
            "string2:string:string2;string1:primarykey64:string1",
            //Attribute schema
            "long1;string0;string1;string2",
            //Summary schema
            "string1" );
}

void IndexBuilderExceptionTest::CaseTearDown()
{
}

void IndexBuilderExceptionTest::TestDumpSegmentFailed()
{
    config::IndexPartitionOptions options;
    options.SetIsOnline(false);
    options.GetBuildConfig().enablePackageFile = false;
    options.GetBuildConfig().usePathMetaCache = false;

    // SegmentWriter use 144117306B?
    util::QuotaControlPtr quotaControl(new util::QuotaControl(1000 * 1024 * 1024));
    IndexBuilderPtr builder(new IndexBuilder(mRootDir, options, 
                    mSchema, quotaControl, NULL));
    builder->Init();

    string docString = "cmd=add,string1=pk1,string2=a,long1=4;"
                       "cmd=add,string1=pk2,string2=a,long1=5;"
                       "cmd=add,string1=pk3,string2=a,long1=6;";
    vector<NormalDocumentPtr> docs = DocumentCreator::CreateDocuments(mSchema, docString);
    for (size_t i = 0; i < docs.size(); ++i)
    {
        DocumentPtr indexDoc = docs[i];
        ASSERT_TRUE(builder->Build(indexDoc));
    }

    string segPath = PathUtil::JoinPath(mRootDir, "segment_0_level_0");
    FileSystemWrapper::SetError(FileSystemWrapper::IS_EXIST_ERROR, segPath);
    ASSERT_THROW(builder->DumpSegment(), NonExistException);

    docString = "cmd=add,string1=pk4,string2=a,long1=7;";
    docs = DocumentCreator::CreateDocuments(mSchema, docString);
    ASSERT_EQ((size_t)1, docs.size());
    ASSERT_THROW(builder->Build(docs[0]), InconsistentStateException);
    //PSM will call EndIndex twice...
    //ASSERT_THROW(builder->EndIndex(), InconsistentStateException);
    FileSystemWrapper::ClearError();
}

IE_NAMESPACE_END(partition);

