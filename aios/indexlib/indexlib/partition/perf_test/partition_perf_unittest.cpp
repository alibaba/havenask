#include "indexlib/partition/perf_test/partition_perf_unittest.h"
#include "indexlib/index_base/patch/single_part_patch_finder.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/index_base/segment/segment_directory.h"
#include "indexlib/partition/online_partition.h"
#include "indexlib/partition/online_partition_reader.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/file_system/indexlib_file_system_impl.h"
#include "indexlib/file_system/normal_file_reader.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/storage/file_system_wrapper.h"
#include <autil/TimeUtility.h>

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, PartitionPerfTest);

PartitionPerfTest::PartitionPerfTest()
{
}

PartitionPerfTest::~PartitionPerfTest()
{
}

void PartitionPerfTest::CaseSetUp()
{
    mRootDir = GET_TEST_DATA_PATH();
}

void PartitionPerfTest::CaseTearDown()
{
}

string PartitionPerfTest::MakeOneDoc(const IndexPartitionSchemaPtr& schema, 
                                     const string& cmdType)
{
    stringstream ss;
    ss << "cmd=" << cmdType;
    const FieldSchemaPtr& fieldSchema = schema->GetFieldSchema();
    FieldSchema::Iterator iter = fieldSchema->Begin();
    for (; iter != fieldSchema->End(); iter++)
    {
        const FieldConfigPtr& fieldConfig = *iter;
        ss << "," << fieldConfig->GetFieldName() << "=100";
    }

    const IndexPartitionSchemaPtr& subSchema = schema->GetSubIndexPartitionSchema();
    if (subSchema)
    {
        const FieldSchemaPtr& subFieldSchema = subSchema->GetFieldSchema();
        FieldSchema::Iterator subIter = subFieldSchema->Begin();
        for (; subIter != subFieldSchema->End(); subIter++)
        {
            const FieldConfigPtr& subFieldConfig = *subIter;
            ss << "," << subFieldConfig->GetFieldName() << "=10";
        }
    }
    ss << ";";
    return ss.str();
}

void PartitionPerfTest::TestInitReaderPerf()
{
    IndexPartitionOptions options;
    options.GetBuildConfig().maxDocCount = 1;

    string rootDir = mRootDir;
    for (size_t i = 0; i < 20; i++)
    {
        rootDir = rootDir + "testDir1234567890/";
    }
    FileSystemWrapper::MkDir(rootDir, true);

    IndexPartitionSchemaPtr schema = SchemaAdapter::LoadSchema(
                TEST_DATA_PATH, "tmall_searcher_schema.json");
    assert(schema);
    string docString;
    for (size_t i = 0; i < 30; i++)
    {
        docString += MakeOneDoc(schema, "add");
    }

    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(schema, options, rootDir));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "", ""));

    IndexPartitionSchemaPtr loadedSchema = 
        SchemaAdapter::LoadAndRewritePartitionSchema(rootDir, options);

    file_system::IndexlibFileSystemPtr fileSystem(
            new file_system::IndexlibFileSystemImpl(rootDir));
    file_system::FileSystemOptions fsOptions;
    LoadConfigList loadConfigList;
    fsOptions.loadConfigList = loadConfigList;
    fsOptions.enableAsyncFlush = false;
    fsOptions.needFlush = true;
    fsOptions.useCache = true;
    util::MemoryQuotaControllerPtr mqc(
            new util::MemoryQuotaController(std::numeric_limits<int64_t>::max()));
    util::PartitionMemoryQuotaControllerPtr quotaController(
            new util::PartitionMemoryQuotaController(mqc));
    fsOptions.memoryQuotaController = quotaController;
    fileSystem->Init(fsOptions);

    OnDiskPartitionDataPtr partitionData = 
        PartitionDataCreator::CreateOnDiskPartitionData(
                fileSystem, INVALID_VERSION, "", true, true);

    OnlinePartitionReaderPtr reader(new OnlinePartitionReader(options, loadedSchema, util::SearchCachePartitionWrapperPtr()));
    reader->Open(partitionData);

    int64_t beginTime = TimeUtility::currentTime();

    OnlinePartitionReaderPtr newReader(new OnlinePartitionReader(options, loadedSchema, util::SearchCachePartitionWrapperPtr()));
    newReader->Open(partitionData);
    
    int64_t interval = TimeUtility::currentTime() - beginTime;
    cout << "***** time interval : " << interval / 1000 << "ms, "
         << "***** versionId : " << newReader->GetVersion().GetVersionId() << endl;

    cout << "file count:" << fileSystem->GetFileSystemMetrics().GetLocalStorageMetrics().GetTotalFileCount() << endl;
}

void PartitionPerfTest::TestFindAttributePatchPerf()
{
    IndexPartitionSchemaPtr schema = SchemaAdapter::LoadSchema(
                TEST_DATA_PATH, "tmall_searcher_schema.json");
    assert(schema);
    string docString;
    for (size_t i = 0; i < 10; i++)
    {
        docString += MakeOneDoc(schema, "add");
        docString += MakeOneDoc(schema, "update_field");
    }

    string rootDir = mRootDir;
    IndexPartitionOptions options;
    options.GetBuildConfig().maxDocCount = 1;

    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(schema, options, rootDir));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "", ""));

    OnlinePartition onlinePartition;
    onlinePartition.Open(rootDir, "", schema, options);
    
    const SegmentDirectoryPtr& segmentDirectory = 
        onlinePartition.GetPartitionData()->GetSegmentDirectory();
    
    SinglePartPatchFinder finder;
    finder.Init(segmentDirectory);
    
    size_t count = 0;
    int64_t beginTime = TimeUtility::currentTime();
    const AttributeSchemaPtr& attrSchema = schema->GetAttributeSchema();
    AttributeSchema::Iterator iter = attrSchema->Begin();
    for (; iter != attrSchema->End(); iter++)
    {
        if (!(*iter)->IsAttributeUpdatable())
        {
            continue;
        }

        AttrPatchInfos attrPatchInfos;
        finder.FindAttrPatchFiles((*iter)->GetAttrName(),
                (*iter)->GetOwnerModifyOperationId(), attrPatchInfos);
        count += attrPatchInfos.size();
    }

    const IndexPartitionSchemaPtr& subSchema = schema->GetSubIndexPartitionSchema();
    if (subSchema)
    {
        SinglePartPatchFinder finder;
        finder.Init(segmentDirectory->GetSubSegmentDirectory());

        const AttributeSchemaPtr& attrSchema = subSchema->GetAttributeSchema();
        AttributeSchema::Iterator iter = attrSchema->Begin();
        for (; iter != attrSchema->End(); iter++)
        {
            if (!(*iter)->IsAttributeUpdatable())
            {
                continue;
            }
            AttrPatchInfos attrPatchInfos;
            finder.FindAttrPatchFiles((*iter)->GetAttrName(),
                    (*iter)->GetOwnerModifyOperationId(), attrPatchInfos);
            count += attrPatchInfos.size();
        }
    }
    int64_t interval = TimeUtility::currentTime() - beginTime;
    cout << "***** time interval : " << interval / 1000 << "ms, "
         << "***** count:" << count<< endl;
}

void PartitionPerfTest::TestFindDeletionmapFilePerf()
{
    IndexPartitionSchemaPtr schema = SchemaAdapter::LoadSchema(
                TEST_DATA_PATH, "tmall_searcher_schema.json");
    assert(schema);
    string docString;
    for (size_t i = 0; i < 10; i++)
    {
        docString += MakeOneDoc(schema, "add");
    }

    IndexPartitionOptions options;
    options.GetBuildConfig().maxDocCount = 1;

    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(schema, options, mRootDir));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "", ""));

    OnlinePartition onlinePartition;
    onlinePartition.Open(mRootDir, "", schema, options);
    
    const SegmentDirectoryPtr& segmentDirectory = 
        onlinePartition.GetPartitionData()->GetSegmentDirectory();
    
    SinglePartPatchFinder finder;
    finder.Init(segmentDirectory);
    
    size_t count = 0;
    int64_t beginTime = TimeUtility::currentTime();

    DeletePatchInfos delPatchInfos;
    finder.FindDeletionMapFiles(delPatchInfos);
    count += delPatchInfos.size();

    const IndexPartitionSchemaPtr& subSchema = schema->GetSubIndexPartitionSchema();
    if (subSchema)
    {
        SinglePartPatchFinder finder;
        finder.Init(segmentDirectory->GetSubSegmentDirectory());

        DeletePatchInfos delPatchInfos;
        finder.FindDeletionMapFiles(delPatchInfos);
        count += delPatchInfos.size();
    }
    int64_t interval = TimeUtility::currentTime() - beginTime;
    cout << "***** time interval : " << interval / 1000 << "ms, "
         << "***** count:" << count<< endl;
}

void PartitionPerfTest::TestUpdatePartitionInfo()
{
    string field = "pkstr:string;text1:text;long1:uint32;multi_long:uint32:true;"
                   "updatable_multi_long:uint32:true:true;";
    string index = "pk:primarykey64:pkstr;pack1:pack:text1;";
    string attr = "long1;multi_long;updatable_multi_long;";
    string summary = "pkstr;";

    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, 
            attr, summary);

    IndexPartitionSchemaPtr subSchema = SchemaMaker::MakeSchema(
            "substr1:string;subpkstr:string;sub_long:uint32;",
            "subindex1:string:substr1;sub_pk:primarykey64:subpkstr",
            "substr1;subpkstr;sub_long;",
            "");
    schema->SetSubIndexPartitionSchema(subSchema);

    IndexPartitionOptions options;
    PartitionStateMachine psm;
    psm.Init(schema, options, mRootDir);

    string docString = "cmd=add,pkstr=hello,ts=0,subpkstr=sub1^sub2,substr1=s1^s2";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, docString, "", ""));
    const IndexPartitionPtr& indexPart = psm.GetIndexPartition();
    OnlinePartitionPtr onlinePart = DYNAMIC_POINTER_CAST(
            OnlinePartition, indexPart);
    const PartitionDataPtr& partData = onlinePart->GetPartitionData();

    int64_t beginTime = TimeUtility::currentTime();

    const size_t times = 100000;
    for (size_t i = 0; i < times; ++i)
    {
        partData->UpdatePartitionInfo();
    }

    int64_t interval = TimeUtility::currentTime() - beginTime;
    cout << "***** time interval : " << interval / 1000 << "ms, "
         << "***** times : " << times << endl;
}

void PartitionPerfTest::TestMap()
{
    typedef vector<string> FilePathVector;
    typedef map<string, int32_t> FilePathMap;
    //typedef std::tr1::unordered_map<std::string, int32_t> FilePathMap;

    string rootDir = mRootDir;
    for (size_t i = 0; i < 20; i++)
    {
        rootDir = rootDir + "testDir1234567890/";
    }

    #define COUNT 15600

    FilePathVector filePaths;
    FilePathMap fileMap;
    for (size_t i = 0; i < COUNT; i++)
    {
        string path = rootDir + StringUtil::toString(random()) + "_" + StringUtil::toString(i);
        filePaths.push_back(path);
        fileMap[path] = (int32_t)i;
    }

    int64_t beginTime = TimeUtility::currentTime();
    int32_t value = 0;
    FilePathMap::iterator iter;
    for (size_t i = 0; i < COUNT; i++)
    {
        iter = fileMap.find(filePaths[i]);
        if (iter != fileMap.end())
        {
            value = iter->second;
        }
    }

    int64_t interval = TimeUtility::currentTime() - beginTime;
    cout << "***** time interval : " << interval / 1000 << "ms, "
         << "***** value : " << value << endl;
}

IE_NAMESPACE_END(partition);

