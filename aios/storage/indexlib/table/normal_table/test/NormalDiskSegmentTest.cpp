#include "indexlib/table/normal_table/NormalDiskSegment.h"

#include "indexlib/base/MemoryQuotaController.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/framework/MetricsManager.h"
#include "indexlib/framework/SegmentMeta.h"
#include "indexlib/framework/mock/MockTabletSchema.h"
#include "indexlib/index/IIndexFactory.h"
#include "indexlib/index/IIndexReader.h"
#include "indexlib/index/IndexFactoryCreator.h"
#include "indexlib/index/mock/MockIndexConfig.h"
#include "indexlib/index/mock/MockIndexFactory.h"
#include "indexlib/table/normal_table/virtual_attribute/Common.h"
#include "unittest/unittest.h"

namespace indexlibv2::table {

class NormalDiskSegmentTest : public TESTBASE
{
public:
    NormalDiskSegmentTest() = default;
    ~NormalDiskSegmentTest() = default;

public:
    void setUp() override;
    void tearDown() override;
    void InnerTestAddNewVirtualAttr(segmentid_t segmentId, bool success);

private:
    std::shared_ptr<indexlib::file_system::Directory> _rootDir;
};

void NormalDiskSegmentTest::setUp()
{
    indexlib::file_system::FileSystemOptions fsOptions;
    std::string rootPath = GET_TEMP_DATA_PATH();
    auto fs = indexlib::file_system::FileSystemCreator::Create("online", rootPath, fsOptions).GetOrThrow();
    _rootDir = indexlib::file_system::Directory::Get(fs);
}

void NormalDiskSegmentTest::tearDown() {}

TEST_F(NormalDiskSegmentTest, TestSimpleProcess)
{
    std::vector<std::shared_ptr<config::IIndexConfig>> indexConfigs;
    auto metricsManager = std::make_shared<framework::MetricsManager>("", nullptr);
    framework::BuildResource buildResource;
    buildResource.metricsManager = metricsManager.get();

    auto indexConfig = std::make_shared<config::MockIndexConfig>();
    std::string indexType = "operation_log";
    EXPECT_CALL(*indexConfig, GetIndexType).WillRepeatedly(testing::ReturnRef(indexType));
    const std::string indexName = "operation_log";
    EXPECT_CALL(*indexConfig, GetIndexName).WillRepeatedly(testing::ReturnRef(indexName));
    indexConfigs.push_back(indexConfig);
    auto schema = std::make_shared<framework::MockTabletSchema>();
    EXPECT_CALL(*schema, GetIndexConfigs()).WillRepeatedly(testing::Return(indexConfigs));
    framework::SegmentMeta segmentMeta(0);
    std::string segmentDirStr = "segment_0_level_0";
    _rootDir->MakeDirectory(segmentDirStr);
    segmentMeta.segmentDir = _rootDir->GetDirectory(segmentDirStr, /*throwExceptionIfNotExist=*/false);

    // factory is not existed, index type is operation_log
    auto diskSegment1 = std::make_unique<NormalDiskSegment>(schema, segmentMeta, buildResource);
    ASSERT_TRUE(diskSegment1->OpenIndexer(indexConfig).first.IsOK());

    auto tabletFactoryCreator = index::IndexFactoryCreator::GetInstance();
    tabletFactoryCreator->Clean();

    // index dir not exist, factory is existed, index type is operation_log
    auto mockFactory = new index::MockIndexFactory();
    EXPECT_CALL(*mockFactory, GetIndexPath).WillRepeatedly(Return("operation_log"));
    tabletFactoryCreator->Register(indexType, mockFactory);
    auto diskSegment2 = std::make_unique<NormalDiskSegment>(schema, segmentMeta, buildResource);
    ASSERT_TRUE(diskSegment2->OpenIndexer(indexConfig).first.IsOK());
    ASSERT_EQ(0, diskSegment2->EstimateMemUsed(schema));

    // factory is existed, type is primarykey
    std::string indexType2 = "primarykey";
    EXPECT_CALL(*indexConfig, GetIndexType).WillRepeatedly(testing::ReturnRef(indexType2));
    const std::string indexName2 = "primarykey_index";
    EXPECT_CALL(*indexConfig, GetIndexName).WillRepeatedly(testing::ReturnRef(indexName2));
    auto mockFactory2 = new index::MockIndexFactory();
    EXPECT_CALL(*mockFactory2, GetIndexPath).WillRepeatedly(Return("operation_log"));
    tabletFactoryCreator->Register(indexType2, mockFactory2);
    auto diskSegment3 = std::make_unique<NormalDiskSegment>(schema, segmentMeta, buildResource);
    ASSERT_FALSE(diskSegment3->OpenIndexer(indexConfig).first.IsOK());

    tabletFactoryCreator->Clean();
}

void NormalDiskSegmentTest::InnerTestAddNewVirtualAttr(segmentid_t segmentId, bool success)
{
    std::vector<std::shared_ptr<config::IIndexConfig>> indexConfigs;
    auto metricsManager = std::make_shared<framework::MetricsManager>("", nullptr);
    framework::BuildResource buildResource;
    buildResource.metricsManager = metricsManager.get();

    auto indexConfig = std::make_shared<config::MockIndexConfig>();
    std::string indexType = VIRTUAL_ATTRIBUTE_INDEX_TYPE_STR;
    EXPECT_CALL(*indexConfig, GetIndexType).WillRepeatedly(testing::ReturnRef(indexType));
    const std::string indexName = "concurrent_idx";
    EXPECT_CALL(*indexConfig, GetIndexName).WillRepeatedly(testing::ReturnRef(indexName));
    indexConfigs.push_back(indexConfig);
    auto schema = std::make_shared<framework::MockTabletSchema>();
    EXPECT_CALL(*schema, GetIndexConfigs()).WillRepeatedly(testing::Return(indexConfigs));
    framework::SegmentMeta segmentMeta(segmentId);
    std::string segmentDirStr = "segment_" + std::to_string(segmentId) + "_level_0";
    _rootDir->MakeDirectory(segmentDirStr);
    segmentMeta.segmentDir = _rootDir->GetDirectory(segmentDirStr, /*throwExceptionIfNotExist=*/false);

    auto tabletFactoryCreator = index::IndexFactoryCreator::GetInstance();
    tabletFactoryCreator->Clean();

    auto mockFactory = new index::MockIndexFactory();
    EXPECT_CALL(*mockFactory, GetIndexPath).WillRepeatedly(Return(VIRTUAL_ATTRIBUTE_INDEX_PATH));
    tabletFactoryCreator->Register(indexType, mockFactory);
    auto diskSegment2 = std::make_unique<NormalDiskSegment>(schema, segmentMeta, buildResource);
    auto [st, indexItermVec] = diskSegment2->OpenIndexer(indexConfig);
    if (success) {
        ASSERT_TRUE(st.IsOK()) << segmentDirStr;
        ASSERT_TRUE(indexItermVec.size() == 0) << segmentDirStr;
    } else {
        ASSERT_FALSE(st.IsOK()) << segmentDirStr;
    }
    tabletFactoryCreator->Clean();
}

TEST_F(NormalDiskSegmentTest, TestAddNewVirtualAttr)
{
    InnerTestAddNewVirtualAttr(2, /*success*/ true);
    InnerTestAddNewVirtualAttr(framework::Segment::PUBLIC_SEGMENT_ID_MASK | 2, /*success*/ true);
    InnerTestAddNewVirtualAttr(framework::Segment::PRIVATE_SEGMENT_ID_MASK | 2, /*success*/ false);
}

} // namespace indexlibv2::table
