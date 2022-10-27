#include "indexlib/partition/test/offline_partition_unittest.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/index_meta/partition_meta.h"
#include "indexlib/document/document.h"
#include "indexlib/index/test/partition_schema_maker.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/misc/exception.h"
#include "indexlib/index_define.h"
#include "indexlib/index_base/index_meta/index_format_version.h"
#include "indexlib/partition/modifier/partition_modifier.h"
#include "indexlib/partition/segment/dump_segment_container.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"

using namespace std;
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, OfflinePartitionTest);

namespace
{
class MockOfflinePartition : public OfflinePartition
{
public:
    MOCK_METHOD0(CreatePartitionWriter, OfflinePartitionWriterPtr());
};

class MockOfflinePartitionWriter : public OfflinePartitionWriter
{
public:
    MockOfflinePartitionWriter(
        const config::IndexPartitionOptions& options)
        : OfflinePartitionWriter(options,
                                 DumpSegmentContainerPtr(new DumpSegmentContainer))
    {}
    
    MOCK_METHOD3(Open, void(const IndexPartitionSchemaPtr& ,
                            const PartitionDataPtr&,
                            const PartitionModifierPtr& modifier));
    MOCK_METHOD1(ReOpenNewSegment, void(const PartitionModifierPtr& modifier));
    MOCK_METHOD1(BuildDocument, bool(const DocumentPtr& doc));
    MOCK_METHOD0(DumpSegment, void());
    MOCK_METHOD0(Close, void());
    MOCK_CONST_METHOD0(ReportMetrics, void());
    
};
}

OfflinePartitionTest::OfflinePartitionTest()
{
}

OfflinePartitionTest::~OfflinePartitionTest()
{
}

void OfflinePartitionTest::CaseSetUp()
{
    mRootDir = GET_TEST_DATA_PATH();
    mSchema.reset(new IndexPartitionSchema);
    PartitionSchemaMaker::MakeSchema(mSchema,
            //Field schema
            "string0:string;string1:string;long1:uint32;string2:string:true", 
            //Index schema
            "string2:string:string2;"
            //Primary key index schema
            "pk:primarykey64:string1",
            //Attribute schema
            "long1;string0;string1;string2",
            //Summary schema
            "string1" );
    mOptions.TEST_mQuickExit = true;
}

void OfflinePartitionTest::CaseTearDown()
{
}

void OfflinePartitionTest::TestInvalidOpen()
{
    SCOPED_LOG_LEVEL(FATAL);
    {
        // invalid realtime mode
        OfflinePartition partition;
        // BadParameterException
        ASSERT_EQ(IndexPartition::OS_INDEXLIB_EXCEPTION,
                  partition.Open(mRootDir, "", mSchema, mOptions));

    }

    {
        // invalid root dir
        mOptions.SetIsOnline(false);

        OfflinePartition partition;
        // BadParameterException
        ASSERT_EQ(IndexPartition::OS_INDEXLIB_EXCEPTION,
                  partition.Open("", "", mSchema, mOptions));
    }

    {
        // invalid schema
        mOptions.SetIsOnline(false);

        OfflinePartition partition;
        // BadParameterException
        ASSERT_EQ(IndexPartition::OS_INDEXLIB_EXCEPTION,
                  partition.Open(mRootDir, "", IndexPartitionSchemaPtr(), mOptions));
    }

    {
        mOptions.SetIsOnline(false);

        OfflinePartition fullPartition;
        fullPartition.Open(mRootDir, "", mSchema, mOptions);

        string schemaPath = FileSystemWrapper::JoinPath(mRootDir,
                SCHEMA_FILE_NAME);
        string formatVersionPath = FileSystemWrapper::JoinPath(mRootDir,
                INDEX_FORMAT_VERSION_FILE_NAME);

        {
            // invalid root dir: no schema file
            FileSystemWrapper::Rename(schemaPath, schemaPath + "_tmp");
            OfflinePartition partition;
            ASSERT_EQ(IndexPartition::OS_OK,
                      partition.Open(mRootDir, "", mSchema, mOptions));
            ASSERT_TRUE(FileSystemWrapper::IsExist(schemaPath));
            ASSERT_TRUE(FileSystemWrapper::IsExist(formatVersionPath));
        }

        {
            // invalid root dir: no format version file
            FileSystemWrapper::Rename(formatVersionPath, formatVersionPath + "_tmp");
            OfflinePartition partition;
            ASSERT_EQ(IndexPartition::OS_OK,
                      partition.Open(mRootDir, "", mSchema, mOptions));
            ASSERT_TRUE(FileSystemWrapper::IsExist(schemaPath));
            ASSERT_TRUE(FileSystemWrapper::IsExist(formatVersionPath));
        }
    }
}

void OfflinePartitionTest::TestFullIndexOpen()
{
    // normal case, schema use passed schema
    mOptions.SetIsOnline(false);

    OfflinePartition partition;
    partition.Open(mRootDir, "", mSchema, mOptions);

    ASSERT_TRUE(partition.mPartitionDataHolder.Get());
    ASSERT_TRUE(partition.mWriter);
    ASSERT_NO_THROW(mSchema->AssertEqual(*partition.mSchema));
}

void OfflinePartitionTest::TestIncIndexOpen()
{
    // normal case, use ondisk schema
    IndexPartitionSchemaPtr onDiskSchema(mSchema->Clone());
    onDiskSchema->AddFieldConfig("hello", ft_string);
    string schemaStr = autil::legacy::ToJsonString(*onDiskSchema);
    string schemaPath = FileSystemWrapper::JoinPath(
            mRootDir, SCHEMA_FILE_NAME);

    FileSystemWrapper::AtomicStore(schemaPath, schemaStr);
    IndexFormatVersion binaryVersion(INDEX_FORMAT_VERSION);
    binaryVersion.Store(FileSystemWrapper::JoinPath(
                    mRootDir, INDEX_FORMAT_VERSION_FILE_NAME));

    mOptions.SetIsOnline(false);
        
    OfflinePartition partition;
    partition.Open(mRootDir, "", mSchema, mOptions);
    ASSERT_NO_THROW(onDiskSchema->AssertEqual(*partition.mSchema));
}

void OfflinePartitionTest::TestIncOpenWithIncompatibleFormatVersion()
{
    // case for not incompatible format version
    IndexPartitionSchemaPtr onDiskSchema(mSchema->Clone());
    string schemaStr = autil::legacy::ToJsonString(*onDiskSchema);
    string schemaPath = FileSystemWrapper::JoinPath(
            mRootDir, SCHEMA_FILE_NAME);

    FileSystemWrapper::AtomicStore(schemaPath, schemaStr);
    IndexFormatVersion binaryVersion("1.0.0");
    binaryVersion.Store(FileSystemWrapper::JoinPath(
                    mRootDir, INDEX_FORMAT_VERSION_FILE_NAME));

    mOptions.SetIsOnline(false);
        
    OfflinePartition partition;
    // IndexCollapsedException
    ASSERT_EQ(IndexPartition::OS_INDEXLIB_EXCEPTION, 
              partition.Open(mRootDir, "", mSchema, mOptions));
}

void OfflinePartitionTest::TestSimpleProcess()
{
    mOptions.SetIsOnline(false);
    
    MockOfflinePartitionWriter* mockWriter = 
            new MockOfflinePartitionWriter(mOptions);
    OfflinePartitionWriterPtr offlineWriter(mockWriter);
    MockOfflinePartition partition;

    EXPECT_CALL(partition, CreatePartitionWriter())
        .WillOnce(Return(offlineWriter));
    EXPECT_CALL(*mockWriter, Open(_,_,_))
        .WillOnce(Return());
    EXPECT_CALL(*mockWriter, BuildDocument(_))
        .WillOnce(Return(true));
    EXPECT_CALL(*mockWriter, DumpSegment())
        .WillOnce(Return());
    EXPECT_CALL(*mockWriter, ReOpenNewSegment(_))
        .WillOnce(Return());
    EXPECT_CALL(*mockWriter, ReportMetrics())
        .WillRepeatedly(Return());
    
    partition.Open(mRootDir, "", mSchema, mOptions);
    PartitionWriterPtr writer = partition.GetWriter();
    ASSERT_TRUE(writer);
    ASSERT_TRUE(partition.mPartitionDataHolder.Get());

    DocumentPtr document(new NormalDocument);
    writer->BuildDocument(document);
    writer->DumpSegment();

    partition.ReOpenNewSegment();
}

void OfflinePartitionTest::TestOpenWithInvalidPartitionMeta()
{
    IndexPartitionSchemaPtr schema = SchemaAdapter::LoadSchema(
        string(TEST_DATA_PATH) + "pack_attribute/", "main_schema_with_pack.json");

    mOptions.SetIsOnline(false);
    {
        PartitionMeta partMeta;
        partMeta.AddSortDescription("int8_single", sp_asc);
        partMeta.Store(mRootDir);
    
        OfflinePartition offlinePartition;
        
        // UnSupportedException
        ASSERT_EQ(IndexPartition::OS_INDEXLIB_EXCEPTION, 
                  offlinePartition.Open(mRootDir, "", schema, mOptions));
    }
    {
        PartitionMeta partMeta;
        partMeta.AddSortDescription("non-exist-field", sp_asc);
        partMeta.Store(mRootDir);
    
        OfflinePartition offlinePartition;
        // InconsistentStateException
        ASSERT_EQ(IndexPartition::OS_INDEXLIB_EXCEPTION, 
                  offlinePartition.Open(mRootDir, "", schema, mOptions));
    }
}


IE_NAMESPACE_END(partition);

