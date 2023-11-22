#include "build_service/builder/AsyncBuilderV2.h"

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <stddef.h>
#include <string>
#include <thread>
#include <type_traits>
#include <unistd.h>
#include <vector>

#include "autil/Span.h"
#include "build_service/config/BuilderConfig.h"
#include "build_service/document/test/DocumentTestHelper.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/test/unittest.h"
#include "build_service/util/MemControlStreamQueue.h"
#include "build_service/util/StreamQueue.h"
#include "indexlib/base/Constant.h"
#include "indexlib/base/Status.h"
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/document/DocumentBatch.h"
#include "indexlib/document/ElementaryDocumentBatch.h"
#include "indexlib/document/IDocument.h"
#include "indexlib/document/IDocumentBatch.h"
#include "indexlib/framework/ITablet.h"
#include "indexlib/framework/TabletInfos.h"
#include "indexlib/framework/mock/MockTablet.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/counter/CounterMap.h"
#include "unittest/unittest.h"

namespace {
class FakeSchema : public indexlibv2::config::TabletSchema
{
public:
    FakeSchema() {}
    const std::string& GetTableName() const noexcept override
    {
        static std::string tableName = "test";
        return tableName;
    }
    std::string GetSchemaFileName() const noexcept override { return "schema.json"; }
    bool Serialize(bool, std::string*) const noexcept override { return true; }
    bool Deserialize(const std::string& data) noexcept override { return true; }
};

class SlowAsyncBuilder : public build_service::builder::AsyncBuilderV2
{
public:
    SlowAsyncBuilder(std::shared_ptr<indexlibv2::framework::ITablet> tablet) : AsyncBuilderV2(tablet) {}
    void fillDocBatches(std::vector<std::shared_ptr<indexlibv2::document::IDocumentBatch>>& docBatches) override
    {
        AsyncBuilderV2::fillDocBatches(docBatches);
        sleep(3);
    }
};

class FakeElementaryDocumentBatch : public indexlibv2::document::ElementaryDocumentBatch
{
public:
    std::unique_ptr<indexlibv2::document::IDocumentBatch> Create() const override { return nullptr; }
    const indexlibv2::framework::Locator& GetLastLocator() const override { return _batchLocator; }
    void SetBatchLocator(const indexlibv2::framework::Locator& locator) override {}
    const indexlibv2::framework::Locator& GetBatchLocator() const override { return _batchLocator; }
    int64_t GetMaxTimestamp() const override { return -1; }
    int64_t GetMaxTTL() const override { return -1; }
    void SetMaxTTL(int64_t maxTTL) override {}
    void DropDoc(int64_t docIdx) override {}
    void ReleaseDoc(int64_t docIdx) override { assert(false); }
    bool IsDropped(int64_t docIdx) const override { return false; }
    size_t GetBatchSize() const override { return 0; }
    size_t GetValidDocCount() const override { return 0; }
    size_t EstimateMemory() const override { return 0; }
    size_t GetAddedDocCount() const override { return GetValidDocCount(); }

private:
    indexlibv2::framework::Locator _batchLocator;
};
} // namespace

namespace build_service { namespace builder {

// class AsyncBuilderV2Test : public BUILD_SERVICE_TESTBASE, public ::testing::TestWithParam<bool>
class AsyncBuilderV2Test : public TESTBASE_BASE, public ::testing::TestWithParam<bool>
{
public:
    void SetUp() override { TESTBASE_BASE::SetUp(); }
    void TearDown() override { TESTBASE_BASE::TearDown(); }

    void setUp() override;
    void tearDown() override;

private:
    indexlibv2::framework::MockTablet* _mockTablet;
    std::shared_ptr<indexlibv2::framework::ITablet> _tablet;
    std::shared_ptr<indexlibv2::config::ITabletSchema> _schema;
    std::unique_ptr<indexlibv2::framework::TabletInfos> _infos;
};

INSTANTIATE_TEST_CASE_P(Regroup, AsyncBuilderV2Test, testing::Values(true, false));

void AsyncBuilderV2Test::setUp()
{
    _mockTablet = new indexlibv2::framework::MockTablet;
    _tablet.reset(_mockTablet);

    _schema = std::make_shared<FakeSchema>();
    _infos = std::make_unique<indexlibv2::framework::TabletInfos>();
    auto counterMap = std::make_shared<indexlib::util::CounterMap>();
    _infos->SetCounterMap(counterMap);
    ASSERT_TRUE(_infos->InitCounter(false).IsOK());
    EXPECT_CALL(*_mockTablet, GetTabletSchema()).WillRepeatedly(Return(_schema));
    EXPECT_CALL(*_mockTablet, GetTabletInfos()).WillRepeatedly(Return(_infos.get()));
    EXPECT_CALL(*_mockTablet, Flush()).WillRepeatedly(Return(indexlib::Status::OK()));
}

void AsyncBuilderV2Test::tearDown()
{
    _tablet.reset();
    _schema.reset();
}

namespace {

std::shared_ptr<indexlibv2::document::DocumentBatch> createDocumentBatch(std::map<std::string, std::string> kvs)
{
    auto batch = std::make_shared<indexlibv2::document::DocumentBatch>();
    auto doc = std::make_shared<build_service::document::FakeRawDocument>();
    for (const auto& [key, value] : kvs) {
        doc->setField(autil::StringView(key), autil::StringView(value));
    }
    batch->AddDocument(doc);
    return batch;
}

std::shared_ptr<indexlibv2::document::DocumentBatch>
createDocumentBatchWithMultiDocs(const std::vector<std::map<std::string, std::string>>& docDesc)
{
    auto batch = std::make_shared<indexlibv2::document::DocumentBatch>();
    for (const auto& kvMap : docDesc) {
        auto doc = std::make_shared<build_service::document::FakeRawDocument>();
        for (const auto& [key, value] : kvMap) {
            doc->setField(autil::StringView(key), autil::StringView(value));
        }
        batch->AddDocument(doc);
    }
    return batch;
}

template <typename Duration>
bool timeWait(std::function<bool()> predicate, Duration checkDuration, size_t checkCount)
{
    size_t count = 0;
    for (;;) {
        if (predicate()) {
            return true;
        }
        std::this_thread::sleep_for(checkDuration);
        if (++count >= checkCount) {
            return false;
        }
    }
    return false;
}

} // namespace

TEST_P(AsyncBuilderV2Test, testSimple)
{
    bool needRegroup = GetParam();
    AsyncBuilderV2 builder(_tablet, proto::BuildId(), needRegroup);
    config::BuilderConfig config;
    config.asyncQueueSize = 5;
    config.asyncBuild = true;
    builder.init(config);
    std::vector<indexlibv2::document::IDocument*> docs;
    EXPECT_CALL(*_mockTablet, Build(_)).WillOnce(Return(indexlib::Status::OK()));
    auto rawDoc = createDocumentBatch({{"pk_field", "1"}, {"field", "10"}});
    builder.build(rawDoc);
    builder.stop(INVALID_TIMESTAMP, /*needSeal*/ false, false);
}

TEST_P(AsyncBuilderV2Test, testBuild)
{
    bool needRegroup = GetParam();
    AsyncBuilderV2 builder(_tablet, proto::BuildId(), needRegroup);
    config::BuilderConfig config;
    config.asyncQueueSize = 5;
    config.asyncBuild = true;
    builder.init(config);

    builder._releaseDocQueue->push(createDocumentBatch({{"pk_field", "1"}, {"field", "10"}}));
    builder._releaseDocQueue->push(createDocumentBatch({{"pk_field", "2"}, {"field", "10"}}));

    std::vector<indexlibv2::document::IDocument*> docs;
    EXPECT_CALL(*_mockTablet, Build(_)).WillOnce(Return(indexlib::Status::OK()));

    ASSERT_TRUE(builder.build(createDocumentBatch({{"pk_field", "2"}, {"field", "10"}})));
    usleep(50000);
    ASSERT_EQ((uint32_t)1, builder._releaseDocQueue->size());

    // test has fatal error
    builder.setFatalError();
    ASSERT_FALSE(builder.build(createDocumentBatch({{"pk_field", "3"}, {"field", "10"}})));

    // test stop
    builder.stop(INVALID_TIMESTAMP, false, false);
    ASSERT_FALSE(builder.build(createDocumentBatch({{"pk_field", "4"}, {"field", "10"}})));
}

TEST_P(AsyncBuilderV2Test, testBuildThreadNormal)
{
    bool needRegroup = GetParam();
    AsyncBuilderV2 builder(_tablet, proto::BuildId(), needRegroup);
    config::BuilderConfig config;
    config.asyncQueueSize = 5;
    config.asyncBuild = true;
    builder.init(config);

    std::vector<indexlibv2::document::IDocument*> docs;
    EXPECT_CALL(*_mockTablet, Build(_))
        .WillOnce(Return(indexlib::Status::OK()))
        .WillOnce(Return(indexlib::Status::Uninitialize()));

    std::shared_ptr<indexlibv2::document::IDocumentBatch> doc =
        createDocumentBatch({{"pk_field", "1"}, {"field", "10"}});
    builder._docQueue->push(doc, 0);
    usleep(50000);
    ASSERT_EQ((uint32_t)0, builder._docQueue->size());
    ASSERT_EQ((uint32_t)1, builder._releaseDocQueue->size());
    ASSERT_TRUE(builder._running);
    std::shared_ptr<indexlibv2::document::IDocumentBatch> doc1 =
        createDocumentBatch({{"pk_field", "2"}, {"field", "10"}});
    builder._docQueue->push(doc1, 0);
    usleep(50000);
    ASSERT_EQ((uint32_t)0, builder._docQueue->size());
    ASSERT_EQ((uint32_t)1, builder._releaseDocQueue->size());
    ASSERT_FALSE(builder._running);
    ASSERT_TRUE(builder.needReconstruct());
    ASSERT_FALSE(builder.build(createDocumentBatch({{"pk_field", "3"}, {"field", "10"}})));
}

TEST_P(AsyncBuilderV2Test, testStop)
{
    bool needRegroup = GetParam();
    AsyncBuilderV2 builder(_tablet, proto::BuildId(), needRegroup);
    config::BuilderConfig config;
    config.asyncQueueSize = 5;
    config.asyncBuild = true;
    builder.init(config);

    EXPECT_CALL(*_mockTablet, Build(_))
        .WillOnce(Return(indexlib::Status::OK()))
        .WillOnce(Return(indexlib::Status::OK()))
        .WillOnce(Return(indexlib::Status::OK()));
    ASSERT_TRUE(builder.build(createDocumentBatch({{"pk_field", "1"}, {"field", "10"}})));
    ASSERT_TRUE(builder.build(createDocumentBatch({{"pk_field", "2"}, {"field", "10"}})));
    ASSERT_TRUE(builder.build(createDocumentBatch({{"pk_field", "3"}, {"field", "10"}})));

    builder.stop(INVALID_TIMESTAMP, false, false);
    ASSERT_FALSE(builder._running);
    ASSERT_TRUE(builder._asyncBuildThreadPtr == NULL);
    ASSERT_EQ((uint32_t)0, builder._releaseDocQueue->size());
}

TEST_F(AsyncBuilderV2Test, testInit)
{
    config::BuilderConfig config;
    config.asyncQueueSize = 5;
    config.batchBuildSize = 3;
    config.asyncBuild = true;

    // test init normal
    AsyncBuilderV2 builder(_tablet);
    ASSERT_TRUE(builder.init(config));
    ASSERT_TRUE(builder._running);
    ASSERT_TRUE(builder._asyncBuildThreadPtr != NULL);
    ASSERT_EQ((uint32_t)5, builder._docQueue->capacity());
    // equal to docQueue.size() + batchBuildSize + 2
    ASSERT_EQ((uint32_t)(5 + 3 + 2), builder._releaseDocQueue->capacity());
}

TEST_F(AsyncBuilderV2Test, testBatchBuild)
{
    config::BuilderConfig config;
    config.asyncQueueSize = 10;
    config.batchBuildSize = 3;
    config.asyncBuild = true;
    size_t builtDocCount = 0;
    volatile bool stopped = false;
    auto buildFunc = [&builtDocCount, &stopped](
                         const std::shared_ptr<indexlibv2::document::IDocumentBatch>& batch) -> indexlib::Status {
        if (stopped) {
            return indexlib::Status::Corruption();
        }
        builtDocCount += batch->GetBatchSize();
        return indexlib::Status::OK();
    };
    ON_CALL(*_mockTablet, Build(_)).WillByDefault(Invoke(buildFunc));
    SlowAsyncBuilder builder(_tablet);
    ASSERT_TRUE(builder.init(config));
    for (size_t i = 0; i < 5; ++i) {
        ASSERT_TRUE(builder.build(createDocumentBatch({{"pk_field", "2"}, {"field", "10"}})));
    }
    while (builtDocCount != 5)
        ;
    ASSERT_EQ(5, builtDocCount);
    builder.stop(0, false, false);
    stopped = true;
    ASSERT_EQ(5, builtDocCount);
}

TEST_F(AsyncBuilderV2Test, testBuildThreadPool)
{
    config::BuilderConfig config;
    config.asyncQueueSize = 10;
    config.batchBuildSize = 3;
    config.asyncBuild = true;
    config.consistentModeBuildThreadCount = 3;
    config.inconsistentModeBuildThreadCount = 8;
    size_t builtDocCount = 0;
    volatile bool stopped = false;
    auto buildFunc = [&builtDocCount, &stopped](
                         const std::shared_ptr<indexlibv2::document::IDocumentBatch>& batch) -> indexlib::Status {
        if (stopped) {
            return indexlib::Status::Corruption();
        }
        builtDocCount += batch->GetBatchSize();
        return indexlib::Status::OK();
    };
    ON_CALL(*_mockTablet, Build(_)).WillByDefault(Invoke(buildFunc));
    SlowAsyncBuilder builder(_tablet);
    ASSERT_TRUE(builder.init(config));
    for (size_t i = 0; i < 5; ++i) {
        ASSERT_TRUE(builder.build(createDocumentBatch({{"pk_field", "2"}, {"field", "10"}})));
    }
    builder.switchToConsistentMode();
    for (size_t i = 0; i < 3; ++i) {
        ASSERT_TRUE(builder.build(createDocumentBatch({{"pk_field", "3"}, {"field", "10"}})));
    }

    builder.stop(0, false, false);
    stopped = true;
    ASSERT_EQ(8, builtDocCount);
}

TEST_F(AsyncBuilderV2Test, testSimpleRegroupDisabled)
{
    AsyncBuilderV2 builder(_tablet, proto::BuildId(), false);
    config::BuilderConfig config;
    config.asyncQueueSize = 5;
    config.asyncBuild = true;
    ASSERT_TRUE(builder.init(config));
    ASSERT_EQ(1, builder._regroupBatchSize);
    ASSERT_EQ(false, builder._regroup);

    size_t builtDocCount = 0;
    auto buildFunc =
        [&builtDocCount](const std::shared_ptr<indexlibv2::document::IDocumentBatch>& batch) -> indexlib::Status {
        auto batchSize = batch->GetBatchSize();
        if (batchSize != 5) {
            return indexlib::Status::Corruption();
        }
        builtDocCount += batch->GetBatchSize();
        return indexlib::Status::OK();
    };
    ON_CALL(*_mockTablet, Build(_)).WillByDefault(Invoke(buildFunc));

    for (size_t i = 0; i < 5; ++i) {
        ASSERT_TRUE(builder.build(createDocumentBatchWithMultiDocs({{{"pk_field", "1"}, {"field", "10"}},
                                                                    {{"pk_field", "2"}, {"field", "10"}},
                                                                    {{"pk_field", "3"}, {"field", "10"}},
                                                                    {{"pk_field", "4"}, {"field", "10"}},
                                                                    {{"pk_field", "5"}, {"field", "10"}}})));
    }

    builder.stop(INVALID_TIMESTAMP, /*needSeal*/ false, false);
    ASSERT_EQ(25, builtDocCount);
}

TEST_F(AsyncBuilderV2Test, testCheckInputDocType)
{
    {
        config::BuilderConfig config;
        config.asyncQueueSize = 10;
        config.batchBuildSize = 3;
        config.asyncBuild = true;
        SlowAsyncBuilder builder(_tablet);
        ASSERT_TRUE(builder.init(config));
        auto elementaryDocumentBatch = std::make_shared<FakeElementaryDocumentBatch>();
        ASSERT_FALSE(builder.build(elementaryDocumentBatch));
        ASSERT_FALSE(builder._running);
        ASSERT_TRUE(builder.hasFatalError());
    }
    {
        config::BuilderConfig config;
        config.asyncQueueSize = 10;
        config.batchBuildSize = 3;
        config.asyncBuild = true;
        AsyncBuilderV2 builder(_tablet, proto::BuildId(), false);
        ASSERT_TRUE(builder.init(config));
        auto buildFunc = [](const std::shared_ptr<indexlibv2::document::IDocumentBatch>& batch) -> indexlib::Status {
            return indexlib::Status::OK();
        };
        ON_CALL(*_mockTablet, Build(_)).WillByDefault(Invoke(buildFunc));
        auto elementaryDocumentBatch = std::make_shared<FakeElementaryDocumentBatch>();
        ASSERT_TRUE(builder.build(elementaryDocumentBatch));
        ASSERT_TRUE(builder._running);
        ASSERT_FALSE(builder.hasFatalError());
    }
}

}} // namespace build_service::builder
