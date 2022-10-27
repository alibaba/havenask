#include "indexlib/index/calculator/test/partition_size_calculator_unittest.h"
#include "indexlib/index/calculator/partition_size_calculator.h"
#include "indexlib/index/calculator/segment_lock_size_calculator.h"
#include "indexlib/test/version_maker.h"
#include "indexlib/index/normal/inverted_index/accessor/index_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/index_reader_factory.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/config/primary_key_index_config.h"
#include "indexlib/file_system/in_mem_directory.h"
#include "indexlib/util/path_util.h"
#include "indexlib/util/counter/multi_counter.h"
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);

namespace
{
class MockPartitionSizeCalculator : public PartitionSizeCalculator
{
public:
    MockPartitionSizeCalculator(const file_system::DirectoryPtr& directory,
                                const config::IndexPartitionSchemaPtr& schema)
        : PartitionSizeCalculator(directory, schema, true, plugin::PluginManagerPtr())
    {}

    MOCK_METHOD1(CreateSegmentCalculator, SegmentLockSizeCalculatorPtr(
                    const config::IndexPartitionSchemaPtr& schema));
    
};
}

void PartitionSizeCalculatorTest::CaseSetUp()
{
    string field = "pk:string;cat:uint32";
    string index = "pk:primarykey64:pk";
    string attr = "cat";
    mSchema = SchemaMaker::MakeSchema(field, index, attr, "");

    string subField = "subPk:string;subCat:uint32";
    string subIndex = "subPk:primarykey64:subPk";
    string subAttr = "subCat";
    IndexPartitionSchemaPtr subSchema = SchemaMaker::MakeSchema(
            subField, subIndex, subAttr, "");
    mSchema->SetSubIndexPartitionSchema(subSchema);

    const IndexlibFileSystemPtr& fs = GET_FILE_SYSTEM();
    string fsRoot = fs->GetRootPath();
    fs->MountInMemStorage(PathUtil::JoinPath(fsRoot, "test"));
    mRootDir.reset(new InMemDirectory(PathUtil::JoinPath(fsRoot, "test"), fs));
}

void PartitionSizeCalculatorTest::CaseTearDown()
{
}

namespace {
class MockSegmentLockSizeCalculator : public SegmentLockSizeCalculator
{
public:
    MockSegmentLockSizeCalculator(const config::IndexPartitionSchemaPtr& schema)
        : SegmentLockSizeCalculator(schema, plugin::PluginManagerPtr()) {}
    ~MockSegmentLockSizeCalculator() {}

    MOCK_CONST_METHOD2(CalculateSize,
                       size_t(const file_system::DirectoryPtr&, const MultiCounterPtr&));
};
DEFINE_SHARED_PTR(MockSegmentLockSizeCalculator);
}

void PartitionSizeCalculatorTest::TestCalculateVersionLockSizeWithoutPatch()
{
    {
        TearDown();
        SetUp();
        mSchema->SetSubIndexPartitionSchema(IndexPartitionSchemaPtr());
        PreparePkSegment(0, mSchema, 1, 0);
        Version version = VersionMaker::Make(0, "0");

        MockSegmentLockSizeCalculatorPtr segCal(new MockSegmentLockSizeCalculator(mSchema));
        EXPECT_CALL(*segCal, CalculateSize(_, _))
            .WillOnce(Return(100));

        MockPartitionSizeCalculator cal(mRootDir, mSchema);
        EXPECT_CALL(cal, CreateSegmentCalculator(mSchema))
            .WillOnce(Return(segCal));
        EXPECT_CALL(cal, CreateSegmentCalculator(mSchema->GetSubIndexPartitionSchema()))
            .WillOnce(Return(SegmentLockSizeCalculatorPtr()));
        PartitionDataPtr partitionData = PartitionDataCreator::CreateOnDiskPartitionData(
            mRootDir->GetFileSystem(), version, mRootDir->GetPath(), false, false);
        EXPECT_EQ((size_t)120, cal.CalculateDiffVersionLockSizeWithoutPatch(
                      version, INVALID_VERSION, partitionData, MultiCounterPtr()));
    }
    {
        TearDown();
        SetUp();
        mSchema->SetSubIndexPartitionSchema(IndexPartitionSchemaPtr());
        PreparePkSegment(0, mSchema, 1, 0);
        PreparePkSegment(9, mSchema, 1, 0);
        PreparePkSegment(10, mSchema, 1, 0);
        Version oldVersion = VersionMaker::Make(0, "0,9,10");
        PreparePkSegment(12, mSchema, 1, 0);
        Version newVersion = VersionMaker::Make(1, "0,10,12");

        MockSegmentLockSizeCalculatorPtr segCal(new MockSegmentLockSizeCalculator(mSchema));
        EXPECT_CALL(*segCal, CalculateSize(_, _))
            .WillOnce(Return(100));

        MockPartitionSizeCalculator cal(mRootDir, mSchema);
        EXPECT_CALL(cal, CreateSegmentCalculator(mSchema))
            .WillOnce(Return(segCal));
        EXPECT_CALL(cal, CreateSegmentCalculator(mSchema->GetSubIndexPartitionSchema()))
            .WillOnce(Return(SegmentLockSizeCalculatorPtr()));
        PartitionDataPtr partitionData = PartitionDataCreator::CreateOnDiskPartitionData(
            mRootDir->GetFileSystem(), newVersion, mRootDir->GetPath(), false, false);
        EXPECT_EQ((size_t)108, cal.CalculateDiffVersionLockSizeWithoutPatch(
                      newVersion, oldVersion, partitionData, MultiCounterPtr()));
    }
}

void PartitionSizeCalculatorTest::TestCalculateVersionLockSizeWithoutPatchWithSubSegment()
{
    PreparePkSegment(0, mSchema, 1, 1);
    Version version = VersionMaker::Make(0, "0");
    MockSegmentLockSizeCalculatorPtr segCal(new MockSegmentLockSizeCalculator(mSchema));
    EXPECT_CALL(*segCal, CalculateSize(_, _))
        .WillOnce(Return(100));
    MockSegmentLockSizeCalculatorPtr subSegCal(new MockSegmentLockSizeCalculator(mSchema));
    EXPECT_CALL(*subSegCal, CalculateSize(_, _))
        .WillOnce(Return(200));

    MockPartitionSizeCalculator cal(mRootDir, mSchema);
    EXPECT_CALL(cal, CreateSegmentCalculator(mSchema))
        .WillOnce(Return(segCal));
    EXPECT_CALL(cal, CreateSegmentCalculator(mSchema->GetSubIndexPartitionSchema()))
        .WillOnce(Return(subSegCal));
    PartitionDataPtr partitionData =  
            PartitionDataCreator::CreateOnDiskPartitionData(
                mRootDir->GetFileSystem(), version,
                mRootDir->GetPath(), 
                true, false);
       
    //mainpk = subpk = 12(pk data) + 8(pk attribute)
    ASSERT_EQ((size_t)340,
              cal.CalculateDiffVersionLockSizeWithoutPatch(version, INVALID_VERSION,
                  partitionData));
}

void PartitionSizeCalculatorTest::TestCalculatePkSize()
{
    mSchema->SetSubIndexPartitionSchema(IndexPartitionSchemaPtr());
    PreparePkSegment(0, mSchema, 1, 0);
    Version version = VersionMaker::Make(1, "0");
    version.Store(mRootDir, false);
    PartitionSizeCalculator calculator(mRootDir, mSchema, true, plugin::PluginManagerPtr());
    PartitionDataPtr partitionData = 
        PartitionDataCreator::CreateOnDiskPartitionData(mRootDir->GetFileSystem(),
                mSchema, INVALID_VERSION, mRootDir->GetPath());
    Version invalidVersion;
    MultiCounterPtr counter(new MultiCounter(""));
    ASSERT_EQ((size_t)20, calculator.CalculatePkSize(partitionData, invalidVersion, counter));
    ASSERT_EQ((size_t)20, counter->Sum());
    IndexConfigPtr indexConfig = mSchema->GetIndexSchema()->GetPrimaryKeyIndexConfig();
    IndexReaderPtr indexReader(IndexReaderFactory::CreateIndexReader(
                    indexConfig->GetIndexType()));
    indexReader->Open(indexConfig, partitionData);

    counter.reset(new MultiCounter(""));
    //test force reopen: version is invalid version, calculate all pk files
    ASSERT_EQ((size_t)20, calculator.CalculatePkSize(partitionData, invalidVersion, counter));
    ASSERT_EQ((size_t)20, counter->Sum());
    counter.reset(new MultiCounter(""));
    ASSERT_EQ((size_t)0, calculator.CalculatePkSize(partitionData, version, counter));
    ASSERT_EQ((size_t)0, counter->Sum());

    //test normal reopen: version is valid version, ignore ignore pk files in cache
    Version incVersion = VersionMaker::Make(0, "");
    counter.reset(new MultiCounter(""));
    ASSERT_EQ((size_t)8, calculator.CalculatePkSize(partitionData, incVersion, counter));
    ASSERT_EQ((size_t)8, counter->Sum());
    counter.reset(new MultiCounter(""));
    ASSERT_EQ((size_t)0, calculator.CalculatePkSize(partitionData, version, counter));
    ASSERT_EQ((size_t)0, counter->Sum());
}

void PartitionSizeCalculatorTest::MakeFile(const DirectoryPtr& dir, 
        const string& fileName, size_t bytes)
{
    string content;
    content.resize(bytes, 'a');
    dir->Store(fileName, content);
}

void PartitionSizeCalculatorTest::PreparePkSegment(
        segmentid_t segId, const IndexPartitionSchemaPtr& schema,
        size_t docCount, size_t subDocCount)
{
    string segName = string("segment_") + StringUtil::toString(segId) + "_level_0";
    DirectoryPtr segDir = mRootDir->MakeDirectory(segName);
    PreparePkSegmentData(segDir, schema, docCount);

    IndexPartitionSchemaPtr subSchema = schema->GetSubIndexPartitionSchema();
    if (subSchema)
    {
        DirectoryPtr subSegDir = segDir->MakeDirectory(SUB_SEGMENT_DIR_NAME);
        PreparePkSegmentData(subSegDir, subSchema, subDocCount);
    }
}

void PartitionSizeCalculatorTest::PreparePkSegmentData(
        const DirectoryPtr& segDir, const IndexPartitionSchemaPtr& schema,
        size_t docCount)
{
    PrimaryKeyIndexConfigPtr pkIndexConfig = DYNAMIC_POINTER_CAST(
            PrimaryKeyIndexConfig,
            schema->GetIndexSchema()->GetPrimaryKeyIndexConfig());
    string pkDirPath = "index/" + pkIndexConfig->GetIndexName();
    DirectoryPtr pkDirectory = segDir->MakeDirectory(pkDirPath);
    MakeFile(pkDirectory, PRIMARY_KEY_DATA_FILE_NAME, 12 * docCount);
    if (pkIndexConfig->HasPrimaryKeyAttribute())
    {
        std::string pkAttrDirName = 
            std::string(PK_ATTRIBUTE_DIR_NAME_PREFIX)
            + '_' + pkIndexConfig->GetFieldConfig()->GetFieldName();
        DirectoryPtr pkAttrDirectory = pkDirectory->MakeDirectory(pkAttrDirName);
        MakeFile(pkAttrDirectory, ATTRIBUTE_DATA_FILE_NAME, 8 * docCount);
    }
    SegmentInfo segInfo;
    segInfo.docCount = docCount;
    segInfo.Store(segDir);
}

void PartitionSizeCalculatorTest::PrepareSegment(segmentid_t segId)
{
    string segName = string("segment_") + StringUtil::toString(segId) + "_level_0";
    DirectoryPtr segDir = mRootDir->MakeDirectory(segName);
    PrepareSegmentData(segId, segDir, "cat", false);

    DirectoryPtr subSegDir = segDir->MakeDirectory(SUB_SEGMENT_DIR_NAME);
    PrepareSegmentData(segId, subSegDir, "subCat", true);
}

void PartitionSizeCalculatorTest::PrepareSegmentData(
        segmentid_t segId, DirectoryPtr segDir, 
        const string& attrName, bool isSub)
{
    DirectoryPtr attrDir = segDir->MakeDirectory(ATTRIBUTE_DIR_NAME);
    DirectoryPtr catAttrDir = attrDir->MakeDirectory(attrName);
    segmentid_t src = segId;
    for (segmentid_t dst = 0; dst < segId; dst++)
    {
        string patchFileName = StringUtil::toString(src) + "_" +
                               StringUtil::toString(dst) + "." + 
                               ATTRIBUTE_PATCH_FILE_NAME;
        size_t length = src + dst;
        if (isSub) { length *= 2; }
        MakeFile(catAttrDir, patchFileName, length);
    }

    SegmentInfo segInfo;
    segInfo.Store(segDir);
}

IE_NAMESPACE_END(index);
