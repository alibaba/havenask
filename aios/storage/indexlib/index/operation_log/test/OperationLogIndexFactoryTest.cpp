#include "indexlib/index/operation_log/OperationLogIndexFactory.h"

#include "indexlib/config/ITabletSchema.h"
#include "indexlib/framework/MetricsManager.h"
#include "indexlib/index/DiskIndexerParameter.h"
#include "indexlib/index/IIndexMerger.h"
#include "indexlib/index/IndexReaderParameter.h"
#include "indexlib/index/MemIndexerParameter.h"
#include "indexlib/index/operation_log/Common.h"
#include "indexlib/index/operation_log/CompressOperationLogMemIndexer.h"
#include "indexlib/index/operation_log/OperationLogConfig.h"
#include "indexlib/index/operation_log/OperationLogIndexReader.h"
#include "indexlib/index/primary_key/Common.h"
#include "indexlib/index/primary_key/config/PrimaryKeyIndexConfig.h"
#include "indexlib/index/test/ConfigAdapter.h"
#include "indexlib/table/normal_table/test/NormalTabletSchemaMaker.h"
#include "indexlib/util/testutil/unittest.h"

namespace indexlib::index {

class OperationLogIndexFactoryTest : public INDEXLIB_TESTBASE
{
public:
    OperationLogIndexFactoryTest();
    ~OperationLogIndexFactoryTest();

    DECLARE_CLASS_NAME(OperationLogIndexFactoryTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestCompress();

private:
    std::shared_ptr<indexlibv2::config::ITabletSchema> _schema;

private:
    AUTIL_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(OperationLogIndexFactoryTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(OperationLogIndexFactoryTest, TestCompress);
AUTIL_LOG_SETUP(indexlib.index, OperationLogIndexFactoryTest);

OperationLogIndexFactoryTest::OperationLogIndexFactoryTest() {}

OperationLogIndexFactoryTest::~OperationLogIndexFactoryTest() {}

void OperationLogIndexFactoryTest::CaseSetUp()
{
    std::string field = "string1:string;string2:string;price:uint32;string3:string:true:-1";
    std::string index = "index2:string:string2;"
                        "pk:primarykey64:string1";

    std::string attribute = "string1;price;string3:true";
    _schema = indexlibv2::table::NormalTabletSchemaMaker::Make(field, index, attribute, "");
}

void OperationLogIndexFactoryTest::CaseTearDown() {}

void OperationLogIndexFactoryTest::TestSimpleProcess()
{
    auto indexFactoryCreator = indexlibv2::index::IndexFactoryCreator::GetInstance();
    auto [status, indexFactory] = indexFactoryCreator->Create(OPERATION_LOG_INDEX_TYPE_STR);
    ASSERT_TRUE(status.IsOK());
    ASSERT_TRUE(indexFactory);
    auto pkConfig =
        std::dynamic_pointer_cast<indexlibv2::config::IIndexConfig>(indexlibv2::index::test::GetPKIndexConfig(_schema));
    std::shared_ptr<OperationLogConfig> opConfig(new OperationLogConfig(2, false));
    opConfig->AddIndexConfigs(indexlibv2::index::PRIMARY_KEY_INDEX_TYPE_STR, {pkConfig});
    indexlibv2::framework::MetricsManager metricsManager("", nullptr);
    indexlibv2::index::DiskIndexerParameter indexerParam;
    indexlibv2::index::MemIndexerParameter memIndexerParam;
    indexlibv2::index::IndexReaderParameter indexReaderParam;
    indexerParam.metricsManager = &metricsManager;
    ASSERT_TRUE(indexFactory->CreateDiskIndexer(opConfig, indexerParam));
    ASSERT_TRUE(indexFactory->CreateMemIndexer(opConfig, memIndexerParam));
    ASSERT_TRUE(indexFactory->CreateIndexReader(opConfig, indexReaderParam));
    ASSERT_TRUE(indexFactory->CreateIndexMerger(opConfig));
}

void OperationLogIndexFactoryTest::TestCompress()
{
    auto indexFactoryCreator = indexlibv2::index::IndexFactoryCreator::GetInstance();
    auto [status, indexFactory] = indexFactoryCreator->Create(OPERATION_LOG_INDEX_TYPE_STR);
    ASSERT_TRUE(status.IsOK());
    ASSERT_TRUE(indexFactory);
    auto pkConfig =
        std::dynamic_pointer_cast<indexlibv2::config::IIndexConfig>(indexlibv2::index::test::GetPKIndexConfig(_schema));
    std::shared_ptr<OperationLogConfig> opConfig(new OperationLogConfig(2, true));
    opConfig->AddIndexConfigs(indexlibv2::index::PRIMARY_KEY_INDEX_TYPE_STR, {pkConfig});
    indexlibv2::framework::MetricsManager metricsManager("", nullptr);
    indexlibv2::index::MemIndexerParameter indexerParam;
    indexerParam.metricsManager = &metricsManager;
    auto memIndexer = indexFactory->CreateMemIndexer(opConfig, indexerParam);
    ASSERT_TRUE(memIndexer);
    auto compressIndexer = std::dynamic_pointer_cast<CompressOperationLogMemIndexer>(memIndexer);
    ASSERT_TRUE(compressIndexer);
}

} // namespace indexlib::index
