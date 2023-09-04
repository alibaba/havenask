#include "indexlib/index/primary_key/PrimaryKeyIndexFactory.h"

#include "indexlib/index/IIndexMerger.h"
#include "indexlib/index/IIndexReader.h"
#include "indexlib/index/IndexFactoryCreator.h"
#include "indexlib/index/IndexerParameter.h"
#include "indexlib/index/primary_key/Common.h"
#include "indexlib/index/primary_key/config/PrimaryKeyIndexConfig.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;

namespace indexlibv2 { namespace index {

class PrimaryKeyIndexFactoryTest : public indexlib::INDEXLIB_TESTBASE
{
public:
    PrimaryKeyIndexFactoryTest();
    ~PrimaryKeyIndexFactoryTest();

    DECLARE_CLASS_NAME(PrimaryKeyIndexFactoryTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    AUTIL_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(PrimaryKeyIndexFactoryTest, TestSimpleProcess);
AUTIL_LOG_SETUP(indexlib.index, PrimaryKeyIndexFactoryTest);

PrimaryKeyIndexFactoryTest::PrimaryKeyIndexFactoryTest() {}

PrimaryKeyIndexFactoryTest::~PrimaryKeyIndexFactoryTest() {}

void PrimaryKeyIndexFactoryTest::CaseSetUp() {}

void PrimaryKeyIndexFactoryTest::CaseTearDown() {}

void PrimaryKeyIndexFactoryTest::TestSimpleProcess()
{
    std::shared_ptr<config::IIndexConfig> config(new indexlibv2::index::PrimaryKeyIndexConfig("pk", it_primarykey64));
    auto indexFactoryCreator = IndexFactoryCreator::GetInstance();
    auto [status, indexFactory] = indexFactoryCreator->Create(config->GetIndexType());
    ASSERT_TRUE(status.IsOK());
    ASSERT_EQ(typeid(PrimaryKeyIndexFactory), typeid(*indexFactory));
    IndexerParameter indexParam;
    ASSERT_TRUE(indexFactory->CreateDiskIndexer(config, indexParam));
    ASSERT_TRUE(indexFactory->CreateMemIndexer(config, indexParam));
    ASSERT_TRUE(indexFactory->CreateIndexReader(config, indexParam));
    ASSERT_TRUE(indexFactory->CreateIndexMerger(config));
    std::string jsonStr = R"(
    {
               "format_version_id":0,
                "index_fields":"uniqueId",
                "index_name":"uniqueId",
                "index_type":"PRIMARYKEY64",
                "pk_data_block_size":4096,
                "pk_hash_type":"default_hash",
                "pk_storage_type":"sort_array"
     }
    )";
    auto any = autil::legacy::json::ParseJson(jsonStr);
    ASSERT_TRUE(indexFactory->CreateIndexConfig(any));
    ASSERT_EQ(std::string("index"), indexFactory->GetIndexPath());
}

}} // namespace indexlibv2::index
