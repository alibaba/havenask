#include "indexlib/index/IndexFactoryCreator.h"

#include "indexlib/config/IIndexConfig.h"
#include "indexlib/index/IIndexFactory.h"
#include "indexlib/index/IIndexMerger.h"
#include "indexlib/index/IIndexReader.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;

namespace indexlibv2 { namespace index {

class IndexFactoryCreatorTest : public indexlib::INDEXLIB_TESTBASE
{
public:
    IndexFactoryCreatorTest();
    ~IndexFactoryCreatorTest();

    DECLARE_CLASS_NAME(IndexFactoryCreatorTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    AUTIL_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(IndexFactoryCreatorTest, TestSimpleProcess);
AUTIL_LOG_SETUP(indexlib.index, IndexFactoryCreatorTest);

IndexFactoryCreatorTest::IndexFactoryCreatorTest() {}

IndexFactoryCreatorTest::~IndexFactoryCreatorTest() {}

void IndexFactoryCreatorTest::CaseSetUp() {}

void IndexFactoryCreatorTest::CaseTearDown() {}

class IndexFactoryA : public IIndexFactory
{
public:
    std::shared_ptr<IDiskIndexer> CreateDiskIndexer(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                                    const IndexerParameter& indexerParam) const override
    {
        return nullptr;
    }
    std::shared_ptr<IMemIndexer> CreateMemIndexer(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                                  const IndexerParameter& indexerParam) const override
    {
        return nullptr;
    }
    std::unique_ptr<IIndexReader> CreateIndexReader(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                                    const IndexerParameter& indexerParam) const override
    {
        return nullptr;
    }
    std::unique_ptr<IIndexMerger>
    CreateIndexMerger(const std::shared_ptr<config::IIndexConfig>& indexConfig) const override
    {
        return nullptr;
    }
    std::unique_ptr<config::IIndexConfig> CreateIndexConfig(const autil::legacy::Any& any) const override
    {
        return nullptr;
    }
    std::string GetIndexPath() const override { return ""; }
    std::unique_ptr<document::IIndexFieldsParser> CreateIndexFieldsParser() override { return nullptr; }
};

class IndexFactoryB : public IIndexFactory
{
public:
    std::shared_ptr<IDiskIndexer> CreateDiskIndexer(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                                    const IndexerParameter& indexerParam) const override
    {
        return nullptr;
    }
    std::shared_ptr<IMemIndexer> CreateMemIndexer(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                                  const IndexerParameter& indexerParam) const override
    {
        return nullptr;
    }
    std::unique_ptr<IIndexReader> CreateIndexReader(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                                    const IndexerParameter& indexerParam) const override
    {
        return nullptr;
    }
    std::unique_ptr<IIndexMerger>
    CreateIndexMerger(const std::shared_ptr<config::IIndexConfig>& indexConfig) const override
    {
        return nullptr;
    }
    std::unique_ptr<config::IIndexConfig> CreateIndexConfig(const autil::legacy::Any& any) const override
    {
        return nullptr;
    }
    std::string GetIndexPath() const override { return ""; }
    std::unique_ptr<document::IIndexFieldsParser> CreateIndexFieldsParser() override { return nullptr; }
};

void IndexFactoryCreatorTest::TestSimpleProcess()
{
    auto indexFactoryCreator = indexlibv2::index::IndexFactoryCreator::GetInstance();
    {
        indexFactoryCreator->Register("type_1", new IndexFactoryA);
        auto [status, indexFactory] = indexFactoryCreator->Create("type_1");
        ASSERT_TRUE(status.IsOK());
    }
    {
        indexFactoryCreator->Register("type_1", new IndexFactoryA);
        auto [status, indexFactory] = indexFactoryCreator->Create("type_1");
        ASSERT_TRUE(status.IsOK());
    }
    {
        indexFactoryCreator->Register("type_1", new IndexFactoryB);
        auto [status, indexFactory] = indexFactoryCreator->Create("type_1");
        ASSERT_FALSE(status.IsOK());
    }
}

}} // namespace indexlibv2::index
