#include "indexlib/index/calculator/test/partition_size_calculator_unittest.h"

#include "autil/StringUtil.h"
#include "indexlib/config/primary_key_index_config.h"
#include "indexlib/file_system/MemDirectory.h"
#include "indexlib/index/calculator/partition_size_calculator.h"
#include "indexlib/index/calculator/segment_lock_size_calculator.h"
#include "indexlib/index/inverted_index/InvertedIndexReader.h"
#include "indexlib/index/normal/framework/legacy_index_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/index_reader_factory.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_define.h"
#include "indexlib/partition/on_disk_partition_data.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/test/version_maker.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/counter/MultiCounter.h"

using namespace std;
using namespace autil;
using namespace indexlib::test;
using namespace indexlib::partition;
using namespace indexlib::index_base;
using namespace indexlib::file_system;
using namespace indexlib::config;
using namespace indexlib::util;
using namespace indexlib::index_base;

namespace indexlib { namespace index {

namespace {
class MockPartitionSizeCalculator : public PartitionSizeCalculator
{
public:
    MockPartitionSizeCalculator(const file_system::DirectoryPtr& directory,
                                const config::IndexPartitionSchemaPtr& schema)
        : PartitionSizeCalculator(directory, schema, true, plugin::PluginManagerPtr())
    {
    }

    MOCK_METHOD(SegmentLockSizeCalculatorPtr, CreateSegmentCalculator, (const config::IndexPartitionSchemaPtr& schema),
                (override));
};
} // namespace

void PartitionSizeCalculatorTest::CaseSetUp()
{
    string field = "pk:string;cat:uint32";
    string index = "pk:primarykey64:pk";
    string attr = "cat";
    mSchema = SchemaMaker::MakeSchema(field, index, attr, "");

    string subField = "subPk:string;subCat:uint32";
    string subIndex = "subPk:primarykey64:subPk";
    string subAttr = "subCat";
    IndexPartitionSchemaPtr subSchema = SchemaMaker::MakeSchema(subField, subIndex, subAttr, "");
    mSchema->SetSubIndexPartitionSchema(subSchema);

    // const IFileSystemPtr& fs = GET_FILE_SYSTEM();
    FileSystemOptions fsOptions;
    fsOptions.outputStorage = FSST_MEM;
    auto fs = FileSystemCreator::Create("ut_calc", GET_TEMP_DATA_PATH(), fsOptions).GetOrThrow();
    mRootDir = IDirectory::ToLegacyDirectory(make_shared<MemDirectory>("test", fs));
}

void PartitionSizeCalculatorTest::CaseTearDown() {}

namespace {
class MockSegmentLockSizeCalculator : public SegmentLockSizeCalculator
{
public:
    MockSegmentLockSizeCalculator(const config::IndexPartitionSchemaPtr& schema)
        : SegmentLockSizeCalculator(schema, plugin::PluginManagerPtr())
    {
    }
    ~MockSegmentLockSizeCalculator() {}

    MOCK_METHOD(size_t, CalculateSize, (const file_system::DirectoryPtr&, const MultiCounterPtr&), (const, override));
};
DEFINE_SHARED_PTR(MockSegmentLockSizeCalculator);
} // namespace

void PartitionSizeCalculatorTest::TestCalculateVersionLockSizeWithoutPatch()
{
    {
        tearDown();
        setUp();
        mSchema->SetSubIndexPartitionSchema(IndexPartitionSchemaPtr());
        PreparePkSegment(0, mSchema, 1, 0);
        Version version = VersionMaker::Make(0, "0");

        MockSegmentLockSizeCalculatorPtr segCal(new MockSegmentLockSizeCalculator(mSchema));
        EXPECT_CALL(*segCal, CalculateSize(_, _)).WillOnce(Return(100));

        MockPartitionSizeCalculator cal(mRootDir, mSchema);
        EXPECT_CALL(cal, CreateSegmentCalculator(mSchema)).WillOnce(Return(segCal));
        EXPECT_CALL(cal, CreateSegmentCalculator(mSchema->GetSubIndexPartitionSchema()))
            .WillOnce(Return(SegmentLockSizeCalculatorPtr()));
        PartitionDataPtr partitionData = OnDiskPartitionData::CreateOnDiskPartitionData(
            mRootDir->GetFileSystem(), version, mRootDir->GetLogicalPath(), false, false);
        EXPECT_EQ((size_t)120, cal.CalculateDiffVersionLockSizeWithoutPatch(
                                   version, index_base::Version(INVALID_VERSION), partitionData, MultiCounterPtr()));
    }
    {
        tearDown();
        setUp();
        mSchema->SetSubIndexPartitionSchema(IndexPartitionSchemaPtr());
        PreparePkSegment(0, mSchema, 1, 0);
        PreparePkSegment(9, mSchema, 1, 0);
        PreparePkSegment(10, mSchema, 1, 0);
        Version oldVersion = VersionMaker::Make(0, "0,9,10");
        PreparePkSegment(12, mSchema, 1, 0);
        Version newVersion = VersionMaker::Make(1, "0,10,12");

        MockSegmentLockSizeCalculatorPtr segCal(new MockSegmentLockSizeCalculator(mSchema));
        EXPECT_CALL(*segCal, CalculateSize(_, _)).WillOnce(Return(100));

        MockPartitionSizeCalculator cal(mRootDir, mSchema);
        EXPECT_CALL(cal, CreateSegmentCalculator(mSchema)).WillOnce(Return(segCal));
        EXPECT_CALL(cal, CreateSegmentCalculator(mSchema->GetSubIndexPartitionSchema()))
            .WillOnce(Return(SegmentLockSizeCalculatorPtr()));
        PartitionDataPtr partitionData = OnDiskPartitionData::CreateOnDiskPartitionData(
            mRootDir->GetFileSystem(), newVersion, mRootDir->GetLogicalPath(), false, false);
        EXPECT_EQ((size_t)108, cal.CalculateDiffVersionLockSizeWithoutPatch(newVersion, oldVersion, partitionData,
                                                                            MultiCounterPtr()));
    }
}

void PartitionSizeCalculatorTest::TestCalculateVersionLockSizeWithoutPatchWithSubSegment()
{
    PreparePkSegment(0, mSchema, 1, 1);
    Version version = VersionMaker::Make(0, "0");
    MockSegmentLockSizeCalculatorPtr segCal(new MockSegmentLockSizeCalculator(mSchema));
    EXPECT_CALL(*segCal, CalculateSize(_, _)).WillOnce(Return(100));
    MockSegmentLockSizeCalculatorPtr subSegCal(new MockSegmentLockSizeCalculator(mSchema));
    EXPECT_CALL(*subSegCal, CalculateSize(_, _)).WillOnce(Return(200));

    MockPartitionSizeCalculator cal(mRootDir, mSchema);
    EXPECT_CALL(cal, CreateSegmentCalculator(mSchema)).WillOnce(Return(segCal));
    EXPECT_CALL(cal, CreateSegmentCalculator(mSchema->GetSubIndexPartitionSchema())).WillOnce(Return(subSegCal));
    PartitionDataPtr partitionData = OnDiskPartitionData::CreateOnDiskPartitionData(
        mRootDir->GetFileSystem(), version, mRootDir->GetLogicalPath(), true, false);

    // mainpk = subpk = 12(pk data) + 8(pk attribute)
    ASSERT_EQ((size_t)340, cal.CalculateDiffVersionLockSizeWithoutPatch(version, index_base::Version(INVALID_VERSION),
                                                                        partitionData));
}

void PartitionSizeCalculatorTest::TestCalculatePkSize()
{
    mSchema->SetSubIndexPartitionSchema(IndexPartitionSchemaPtr());
    PreparePkSegment(0, mSchema, 1, 0);
    Version version = VersionMaker::Make(1, "0");
    version.Store(mRootDir, false);
    PartitionSizeCalculator calculator(mRootDir, mSchema, true, plugin::PluginManagerPtr());
    PartitionDataPtr partitionData = OnDiskPartitionData::CreateOnDiskPartitionData(
        mRootDir->GetFileSystem(), mSchema, index_base::Version(INVALID_VERSION), mRootDir->GetLogicalPath());
    Version invalidVersion;
    MultiCounterPtr counter(new MultiCounter(""));
    ASSERT_EQ((size_t)20, calculator.CalculatePkSize(partitionData, invalidVersion, counter));
    ASSERT_EQ((size_t)20, counter->Sum());
    IndexConfigPtr indexConfig = mSchema->GetIndexSchema()->GetPrimaryKeyIndexConfig();
    std::shared_ptr<InvertedIndexReader> indexReader(
        IndexReaderFactory::CreateIndexReader(indexConfig->GetInvertedIndexType(), /*indexMetrics*/ nullptr));
    const auto& legacyReader = std::dynamic_pointer_cast<index::LegacyIndexReaderInterface>(indexReader);
    ASSERT_TRUE(legacyReader);
    legacyReader->Open(indexConfig, partitionData);

    counter.reset(new MultiCounter(""));
    // test force reopen: version is invalid version, calculate all pk files
    ASSERT_EQ((size_t)20, calculator.CalculatePkSize(partitionData, invalidVersion, counter));
    ASSERT_EQ((size_t)20, counter->Sum());
    counter.reset(new MultiCounter(""));
    ASSERT_EQ((size_t)0, calculator.CalculatePkSize(partitionData, version, counter));
    ASSERT_EQ((size_t)0, counter->Sum());

    // test normal reopen: version is valid version, ignore ignore pk files in cache
    Version incVersion = VersionMaker::Make(0, "");
    counter.reset(new MultiCounter(""));
    ASSERT_EQ((size_t)8, calculator.CalculatePkSize(partitionData, incVersion, counter));
    ASSERT_EQ((size_t)8, counter->Sum());
    counter.reset(new MultiCounter(""));
    ASSERT_EQ((size_t)0, calculator.CalculatePkSize(partitionData, version, counter));
    ASSERT_EQ((size_t)0, counter->Sum());
}

void PartitionSizeCalculatorTest::MakeFile(const DirectoryPtr& dir, const string& fileName, size_t bytes)
{
    string content;
    content.resize(bytes, 'a');
    file_system::WriterOption writerOption;
    writerOption.atomicDump = true;
    dir->Store(fileName, content, writerOption);
}

void PartitionSizeCalculatorTest::PreparePkSegment(segmentid_t segId, const IndexPartitionSchemaPtr& schema,
                                                   size_t docCount, size_t subDocCount)
{
    string segName = string("segment_") + StringUtil::toString(segId) + "_level_0";
    DirectoryPtr segDir = mRootDir->MakeDirectory(segName);
    PreparePkSegmentData(segDir, schema, docCount);

    IndexPartitionSchemaPtr subSchema = schema->GetSubIndexPartitionSchema();
    if (subSchema) {
        DirectoryPtr subSegDir = segDir->MakeDirectory(SUB_SEGMENT_DIR_NAME);
        PreparePkSegmentData(subSegDir, subSchema, subDocCount);
    }
}

void PartitionSizeCalculatorTest::PreparePkSegmentData(const DirectoryPtr& segDir,
                                                       const IndexPartitionSchemaPtr& schema, size_t docCount)
{
    PrimaryKeyIndexConfigPtr pkIndexConfig =
        DYNAMIC_POINTER_CAST(PrimaryKeyIndexConfig, schema->GetIndexSchema()->GetPrimaryKeyIndexConfig());
    string pkDirPath = "index/" + pkIndexConfig->GetIndexName();
    DirectoryPtr pkDirectory = segDir->MakeDirectory(pkDirPath);
    MakeFile(pkDirectory, PRIMARY_KEY_DATA_FILE_NAME, 12 * docCount);
    if (pkIndexConfig->HasPrimaryKeyAttribute()) {
        std::string pkAttrDirName =
            std::string(PK_ATTRIBUTE_DIR_NAME_PREFIX) + '_' + pkIndexConfig->GetFieldConfig()->GetFieldName();
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

void PartitionSizeCalculatorTest::PrepareSegmentData(segmentid_t segId, DirectoryPtr segDir, const string& attrName,
                                                     bool isSub)
{
    DirectoryPtr attrDir = segDir->MakeDirectory(ATTRIBUTE_DIR_NAME);
    DirectoryPtr catAttrDir = attrDir->MakeDirectory(attrName);
    segmentid_t src = segId;
    for (segmentid_t dst = 0; dst < segId; dst++) {
        string patchFileName = StringUtil::toString(src) + "_" + StringUtil::toString(dst) + "." + PATCH_FILE_NAME;
        size_t length = src + dst;
        if (isSub) {
            length *= 2;
        }
        MakeFile(catAttrDir, patchFileName, length);
    }

    SegmentInfo segInfo;
    segInfo.Store(segDir);
}
}} // namespace indexlib::index
