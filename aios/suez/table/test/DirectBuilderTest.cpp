#include "suez/table/DirectBuilder.h"

#include <memory>

#include "MockDirectBuilder.h"
#include "autil/legacy/base64.h"
#include "build_service/builder/test/MockBuilderV2.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/processor/test/MockProcessor.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/reader/RawDocumentReaderCreator.h"
#include "build_service/reader/SwiftRawDocumentReader.h"
#include "build_service/reader/test/MockRawDocumentReader.h"
#include "build_service/util/SwiftClientCreator.h"
#include "build_service/workflow/RealtimeBuilderDefine.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/config/UnresolvedSchema.h"
#include "indexlib/document/IDocumentBatch.h"
#include "indexlib/document/kv_document/kv_document.h"
#include "indexlib/framework/TabletInfos.h"
#include "indexlib/framework/mock/MockTablet.h"
#include "indexlib/framework/mock/MockTabletOptions.h"
#include "kmonitor/client/MetricsReporter.h"
#include "suez/common/InnerDef.h"
#include "suez/table/RawDocumentReaderCreatorAdapter.h"
#include "suez/table/wal/CommonDefine.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;

namespace suez {

using build_service::reader::RawDocumentReader;

class MockReaderCreator : public build_service::reader::RawDocumentReaderCreator {
public:
    MockReaderCreator(const build_service::util::SwiftClientCreatorPtr &swiftClientCreator)
        : build_service::reader::RawDocumentReaderCreator(swiftClientCreator, nullptr) {}

public:
    MOCK_METHOD6(create,
                 RawDocumentReader *(const build_service::config::ResourceReaderPtr &,
                                     const build_service::KeyValueMap &,
                                     const build_service::proto::PartitionId &,
                                     indexlib::util::MetricProviderPtr,
                                     const indexlib::util::CounterMapPtr &,
                                     const std::shared_ptr<indexlibv2::config::ITabletSchema> &));
};

struct MockPipeline {
    MockPipeline() {
        mockReader = new build_service::reader::MockRawDocumentReader();
        mockReader->initDocumentFactoryWrapper(build_service::reader::ReaderInitParam());
        mockProcessor = new build_service::processor::MockProcessor();
        mockBuilder = new build_service::builder::MockBuilderV2();
    }

    build_service::reader::MockRawDocumentReader *mockReader = nullptr;
    build_service::processor::MockProcessor *mockProcessor = nullptr;
    build_service::builder::MockBuilderV2 *mockBuilder = nullptr;
};

class DirectBuilderTest : public TESTBASE {
public:
    void setUp() override {
        _configRoot = GET_TEST_DATA_PATH() + "table_test/table_writer/config/0";
        initPid();
        _tablet = std::make_shared<indexlibv2::framework::NiceMockTablet>();
        _tabletOptions = std::make_shared<indexlibv2::framework::NiceMockTabletOptions>();

        auto swiftClientCreator = std::make_shared<build_service::util::SwiftClientCreator>();

        kmonitor::MetricsTags metricsTags;
        auto metricsReporter = std::make_shared<kmonitor::MetricsReporter>("", metricsTags, "");

        build_service::KeyValueMap kvMap;
        kvMap["src_signature"] = "1";
        build_service::workflow::RealtimeInfoWrapper realtimeInfo(std::move(kvMap));
        _builderResource = std::make_unique<build_service::workflow::RealtimeBuilderResource>(
            std::move(metricsReporter),
            nullptr,
            swiftClientCreator,
            build_service::workflow::BuildFlowThreadResource(),
            realtimeInfo);
    }

    void tearDown() override {}

protected:
    std::unique_ptr<DirectBuilder> makeBuilderWithMockPipeline(MockPipeline &mp) {
        auto builder = std::make_unique<DirectBuilder>(_pid, _tablet, *_builderResource, _configRoot, _walConfig);
        builder->_pipeline = std::make_unique<DirectBuilder::Pipeline>();
        builder->_pipeline->reader.reset(mp.mockReader);
        builder->_pipeline->processor.reset(mp.mockProcessor);
        builder->_pipeline->builder.reset(mp.mockBuilder);
        return builder;
    }

    std::unique_ptr<NiceMockDirectBuilder> makeMockBuilder() {
        WALConfig walConfig;
        return std::make_unique<NiceMockDirectBuilder>(_pid, _tablet, *_builderResource, _configRoot, walConfig);
    }

private:
    void initPid() {
        build_service::config::ResourceReader rr(_configRoot);
        auto clusterName = "orc";
        std::string dataTableName;
        ASSERT_TRUE(rr.getDataTableFromClusterName(clusterName, dataTableName));
        _pid.mutable_range()->set_from(0);
        _pid.mutable_range()->set_to(65535);
        _pid.mutable_buildid()->set_generationid(0);
        _pid.mutable_buildid()->set_datatable(dataTableName);
        *_pid.add_clusternames() = clusterName;
    }

protected:
    build_service::proto::PartitionId _pid;
    std::string _configRoot;
    std::shared_ptr<indexlibv2::framework::NiceMockTablet> _tablet;
    std::shared_ptr<indexlibv2::framework::NiceMockTabletOptions> _tabletOptions;
    std::unique_ptr<build_service::workflow::RealtimeBuilderResource> _builderResource;
    WALConfig _walConfig;
};

TEST_F(DirectBuilderTest, testConstructor) {
    DirectBuilder builder(_pid, _tablet, *_builderResource, _configRoot, _walConfig);
    ASSERT_TRUE(builder._readerCreatorAdapter);
    ASSERT_TRUE(builder._srcSignature);
    ASSERT_EQ(1, builder._srcSignature.value());
    ASSERT_FALSE(builder._pipeline);
    ASSERT_EQ(0, builder._totalBuildCount);
    ASSERT_FALSE(builder._buildLoopStop);
    ASSERT_FALSE(builder._needReload);
    ASSERT_TRUE(builder._resourceReader);
}

ACTION_P(AssertCheckpoint, offset, userData) {
    ASSERT_EQ(offset, arg0.offset);
    ASSERT_EQ(userData, arg0.userData);
    ASSERT_EQ(1, arg0.progress.size());
    ASSERT_EQ(0, arg0.progress[0].from);
    ASSERT_EQ(65535, arg0.progress[0].to);
    ASSERT_EQ(offset, arg0.progress[0].offset.first);
}

TEST_F(DirectBuilderTest, testCreateReader) {
    DirectBuilder builder(_pid, _tablet, *_builderResource, _configRoot, _walConfig);
    auto mockReaderCreator = new MockReaderCreator(_builderResource->swiftClientCreator);
    builder._readerCreatorAdapter.reset(
        new RawDocumentReaderCreatorAdapter(_builderResource->swiftClientCreator, _walConfig));
    builder._readerCreatorAdapter->_readerCreator.reset(mockReaderCreator);

    auto resourceReader = builder._resourceReader;
    EXPECT_CALL(*_tablet, GetTabletSchema()).WillRepeatedly(Return(nullptr));
    indexlibv2::framework::TabletInfos tabletInfos;
    EXPECT_CALL(*_tablet, GetTabletInfos()).WillRepeatedly(Return(&tabletInfos));

    // create reader failed
    EXPECT_CALL(*mockReaderCreator, create(_, _, _, _, _, _)).WillOnce(Return(nullptr));
    auto reader = builder.createReader(resourceReader);
    ASSERT_FALSE(reader);

    // locator invalid, do not seek
    auto mockReader = new build_service::reader::MockRawDocumentReader();
    EXPECT_CALL(*mockReader, seek(_)).Times(0);
    EXPECT_CALL(*mockReaderCreator, create(_, _, _, _, _, _)).WillOnce(Return(mockReader));
    reader = builder.createReader(resourceReader);
    ASSERT_TRUE(reader);
    ASSERT_EQ(mockReader, reader.get());

    // source not equal, do not seek
    indexlibv2::framework::Version version;
    indexlibv2::framework::Locator locator(0, 12345);
    version.SetLocator(locator);
    tabletInfos.SetLoadedPublishVersion(version);
    mockReader = new build_service::reader::MockRawDocumentReader();
    EXPECT_CALL(*mockReader, seek(_)).Times(0);
    EXPECT_CALL(*mockReaderCreator, create(_, _, _, _, _, _)).WillOnce(Return(mockReader));
    reader = builder.createReader(resourceReader);
    ASSERT_TRUE(reader);
    ASSERT_EQ(mockReader, reader.get());

    // seek failed
    indexlibv2::framework::Locator buildLocator(1, 100);
    buildLocator.SetUserData("swift");
    tabletInfos.SetBuildLocator(buildLocator);
    mockReader = new build_service::reader::MockRawDocumentReader();
    EXPECT_CALL(*mockReaderCreator, create(_, _, _, _, _, _)).WillOnce(Return(mockReader));
    EXPECT_CALL(*mockReader, seek(_)).WillOnce(DoAll(AssertCheckpoint(100, "swift"), Return(false)));
    reader = builder.createReader(resourceReader);
    ASSERT_FALSE(reader);

    // seek successfully
    mockReader = new build_service::reader::MockRawDocumentReader();
    EXPECT_CALL(*mockReaderCreator, create(_, _, _, _, _, _)).WillOnce(Return(mockReader));
    EXPECT_CALL(*mockReader, seek(_)).WillOnce(DoAll(AssertCheckpoint(100, "swift"), Return(true)));
    reader = builder.createReader(resourceReader);
    ASSERT_TRUE(reader);
}

TEST_F(DirectBuilderTest, testReadFailed) {
    MockPipeline mp;
    auto builder = makeBuilderWithMockPipeline(mp);
    EXPECT_CALL(*mp.mockReader, read(_, _)).WillOnce(Return(RawDocumentReader::ERROR_WAIT));
    ASSERT_FALSE(builder->runBuildPipeline());
    ASSERT_FALSE(builder->_needReload);
    ASSERT_FALSE(builder->_buildLoopStop);
    ASSERT_EQ(0, builder->_totalBuildCount);

    EXPECT_CALL(*mp.mockReader, read(_, _)).WillOnce(Return(RawDocumentReader::ERROR_EXCEPTION));
    ASSERT_FALSE(builder->runBuildPipeline());
    ASSERT_FALSE(builder->_needReload);
    ASSERT_FALSE(builder->_buildLoopStop);
    ASSERT_EQ(0, builder->_totalBuildCount);

    EXPECT_CALL(*mp.mockReader, read(_, _)).WillOnce(Return(RawDocumentReader::ERROR_EOF));
    ASSERT_FALSE(builder->runBuildPipeline());
    ASSERT_FALSE(builder->_needReload);
    ASSERT_FALSE(builder->_buildLoopStop);
    ASSERT_EQ(0, builder->_totalBuildCount);
}

ACTION_P2(doRead, fieldMap, offset, userData) {
    for (const auto &it : fieldMap) {
        arg0.setField(it.first, it.second);
    }
    arg1->offset = offset;
    arg1->userData = userData;
    arg1->progress.push_back(indexlibv2::base::Progress({offset, 0}));
}

ACTION_P(AssertRawDoc, fieldMap) {
    ASSERT_TRUE(arg0);
    ASSERT_EQ(1, arg0->size());
    const auto &rawDoc = (*arg0)[0];
    for (const auto &it : fieldMap) {
        ASSERT_EQ(it.second, rawDoc->getField(it.first));
    }
}

static build_service::document::ProcessedDocumentPtr makeProcessedDoc(bool hasIndexDoc = true) {
    auto processedDoc = std::make_shared<build_service::document::ProcessedDocument>();
    if (hasIndexDoc) {
        auto kvDoc = std::make_shared<indexlib::document::KVDocument>();
        (void)kvDoc->CreateOneDoc();
        auto kvDocBatch = std::make_shared<indexlibv2::document::KVDocumentBatch>();
        auto pool = kvDocBatch->GetPool();
        const auto &kvIndexDoc = *(kvDoc->begin());
        auto docInfo = kvDoc->GetDocInfo();
        auto kvDocV2 = kvIndexDoc.CreateKVDocumentV2(pool, docInfo, kvDoc->GetLocatorV2());
        kvDocBatch->AddDocument(std::move(kvDocV2));
        processedDoc->setDocumentBatch(kvDocBatch);
    }
    return processedDoc;
}

TEST_F(DirectBuilderTest, testProcessFailed) {
    MockPipeline mp;
    auto builder = makeBuilderWithMockPipeline(mp);

    std::map<std::string, string> fieldMap = {{"id", "1"}, {"ha_reserved_timestamp", "1024"}};
    EXPECT_CALL(*mp.mockReader, read(_, _))
        .WillOnce(DoAll(doRead(fieldMap, 1, "swift"), Return(RawDocumentReader::ERROR_NONE)));
    EXPECT_CALL(*mp.mockProcessor, process(_)).WillOnce(DoAll(AssertRawDoc(fieldMap), Return(nullptr)));
    ASSERT_FALSE(builder->runBuildPipeline());
    ASSERT_FALSE(builder->_needReload);
}

ACTION_P(AssertDocLocator, from, to, offset, userData) {
    auto batch = arg0;
    ASSERT_TRUE(batch);
    ASSERT_EQ(1, batch->GetValidDocCount());
    const auto &locator = batch->GetLastLocator();
    ASSERT_EQ(1, locator.GetProgress().size());
    ASSERT_EQ(indexlibv2::base::Progress(from, to, {offset, 0}), locator.GetProgress()[0]);
    ASSERT_EQ(userData, locator.GetUserData());
}

TEST_F(DirectBuilderTest, testBuildFailed) {
    MockPipeline mp;
    auto builder = makeBuilderWithMockPipeline(mp);

    std::map<std::string, string> fieldMap = {{"id", "1"}, {"ha_reserved_timestamp", "1024"}};
    EXPECT_CALL(*mp.mockReader, read(_, _))
        .WillOnce(DoAll(doRead(fieldMap, 1, "swift"), Return(RawDocumentReader::ERROR_NONE)));
    auto processedDocVec = std::make_shared<build_service::document::ProcessedDocumentVec>();
    processedDocVec->push_back(makeProcessedDoc());
    EXPECT_CALL(*mp.mockProcessor, process(_)).WillOnce(DoAll(AssertRawDoc(fieldMap), Return(processedDocVec)));
    EXPECT_CALL(*mp.mockBuilder, doBuild(_))
        .WillOnce(DoAll(AssertDocLocator(0, 65535, 1, "swift"), Return(indexlib::Status::NoMem())))
        .WillOnce(DoAll(AssertDocLocator(0, 65535, 1, "swift"), Return(indexlib::Status::Corruption())));
    ASSERT_FALSE(builder->runBuildPipeline());
    ASSERT_TRUE(mp.mockBuilder->hasFatalError());
    ASSERT_TRUE(builder->_needReload);
}

TEST_F(DirectBuilderTest, testBuildNeedReconstruct) {
    MockPipeline mp;
    auto builder = makeBuilderWithMockPipeline(mp);

    std::map<std::string, string> fieldMap = {{"id", "1"}, {"ha_reserved_timestamp", "1024"}};
    EXPECT_CALL(*mp.mockReader, read(_, _))
        .WillOnce(DoAll(doRead(fieldMap, 1, "swift"), Return(RawDocumentReader::ERROR_NONE)));
    auto processedDocVec = std::make_shared<build_service::document::ProcessedDocumentVec>();
    processedDocVec->push_back(makeProcessedDoc());
    EXPECT_CALL(*mp.mockProcessor, process(_)).WillOnce(DoAll(AssertRawDoc(fieldMap), Return(processedDocVec)));
    EXPECT_CALL(*mp.mockBuilder, doBuild(_))
        .WillOnce(DoAll(AssertDocLocator(0, 65535, 1, "swift"), Return(indexlib::Status::NoMem())))
        .WillOnce(DoAll(AssertDocLocator(0, 65535, 1, "swift"), Return(indexlib::Status::Uninitialize())));
    EXPECT_CALL(*mp.mockBuilder, stop(_, false, true));
    EXPECT_CALL(*mp.mockProcessor, stop(true, false));
    ASSERT_FALSE(builder->runBuildPipeline());
    ASSERT_FALSE(builder->_needReload);
    ASSERT_FALSE(builder->_pipeline);
}

TEST_F(DirectBuilderTest, testRunBuildPipelineSuccess) {
    MockPipeline mp;
    auto builder = makeBuilderWithMockPipeline(mp);

    std::map<std::string, string> fieldMap = {{"id", "1"}, {"ha_reserved_timestamp", "1024"}};
    EXPECT_CALL(*mp.mockReader, read(_, _))
        .WillOnce(DoAll(doRead(fieldMap, 1, "swift"), Return(RawDocumentReader::ERROR_NONE)));
    auto processedDocVec = std::make_shared<build_service::document::ProcessedDocumentVec>();
    processedDocVec->push_back(makeProcessedDoc());
    EXPECT_CALL(*mp.mockProcessor, process(_)).WillOnce(DoAll(AssertRawDoc(fieldMap), Return(processedDocVec)));
    EXPECT_CALL(*mp.mockBuilder, doBuild(_))
        .WillOnce(DoAll(AssertDocLocator(0, 65535, 1, "swift"), Return(indexlib::Status::OK())));
    ASSERT_TRUE(builder->runBuildPipeline());
    ASSERT_FALSE(mp.mockBuilder->hasFatalError());
    ASSERT_FALSE(builder->_needReload);
}

TEST_F(DirectBuilderTest, testNeedCommit) {
    MockPipeline mp;
    auto builder = makeBuilderWithMockPipeline(mp);

    builder->_needReload = true;
    ASSERT_TRUE(builder->needCommit());

    builder->_needReload = false;
    EXPECT_CALL(*_tablet, NeedCommit()).WillOnce(Return(false)).WillOnce(Return(true));
    ASSERT_FALSE(builder->needCommit());
    ASSERT_TRUE(builder->needCommit());
}

TEST_F(DirectBuilderTest, testCommit) {
    MockPipeline mp;
    auto builder = makeBuilderWithMockPipeline(mp);

    auto tabletOptions = std::make_shared<indexlibv2::config::TabletOptions>();
    EXPECT_CALL(*_tablet, GetTabletOptions()).WillRepeatedly(Return(tabletOptions));
    builder->_needReload = true;
    auto ret = builder->commit();
    ASSERT_FALSE(ret.first);

    builder->_needReload = false;
    auto result = std::make_pair(indexlib::Status::IOError(), indexlibv2::framework::VersionMeta());
    EXPECT_CALL(*_tablet, Commit(_)).WillOnce(Return(result));
    ret = builder->commit();
    ASSERT_FALSE(ret.first);

    indexlibv2::framework::Version v(1, 1024);
    indexlibv2::framework::VersionMeta meta(v, 10, 100);
    auto result2 = std::make_pair(indexlib::Status::OK(), meta);
    EXPECT_CALL(*_tablet, Commit(_)).WillOnce(Return(result2));
    ret = builder->commit();
    ASSERT_TRUE(ret.first);
    ASSERT_EQ(1, ret.second.getVersionId());
}

TEST_F(DirectBuilderTest, testStartStopWithoutPipeline) {
    DirectBuilder builder(_pid, _tablet, *_builderResource, _configRoot, _walConfig);
    ASSERT_FALSE(builder._buildThread.joinable());
    indexlibv2::framework::TabletInfos tabletInfos;
    EXPECT_CALL(*_tablet, GetTabletInfos()).WillRepeatedly(Return(&tabletInfos));
    ASSERT_TRUE(builder.start());
    ASSERT_TRUE(builder._buildThread.joinable());
    ASSERT_FALSE(builder._pipeline);
    ASSERT_EQ(0, builder._totalBuildCount);
    builder.stop();
    ASSERT_FALSE(builder._buildThread.joinable());
    ASSERT_TRUE(builder._buildLoopStop);
}

TEST_F(DirectBuilderTest, testStartAndBuildAndStop) {
    MockPipeline mp;
    auto builder = makeBuilderWithMockPipeline(mp);

    std::map<std::string, string> fieldMap = {{"id", "1"}, {"ha_reserved_timestamp", "1024"}};
    EXPECT_CALL(*mp.mockReader, read(_, _))
        .WillOnce(DoAll(doRead(fieldMap, 1, "swift"), Return(RawDocumentReader::ERROR_NONE)))
        .WillOnce(DoAll(doRead(fieldMap, 1, "swift"), Return(RawDocumentReader::ERROR_NONE)))
        .WillOnce(DoAll(doRead(fieldMap, 1, "swift"), Return(RawDocumentReader::ERROR_NONE)));

    auto processedDocVec = std::make_shared<build_service::document::ProcessedDocumentVec>();
    processedDocVec->push_back(makeProcessedDoc());
    EXPECT_CALL(*mp.mockProcessor, process(_))
        .WillOnce(DoAll(AssertRawDoc(fieldMap), Return(processedDocVec)))
        .WillOnce(DoAll(AssertRawDoc(fieldMap), Return(processedDocVec)))
        .WillOnce(DoAll(AssertRawDoc(fieldMap), Return(processedDocVec)));

    EXPECT_CALL(*mp.mockBuilder, doBuild(_))
        .WillOnce(DoAll(AssertDocLocator(0, 65535, 1, "swift"), Return(indexlib::Status::NoMem())))
        .WillOnce(DoAll(AssertDocLocator(0, 65535, 1, "swift"), Return(indexlib::Status::OK())))
        .WillOnce(DoAll(AssertDocLocator(0, 65535, 1, "swift"), Return(indexlib::Status::OK())))
        .WillOnce(DoAll(AssertDocLocator(0, 65535, 1, "swift"), Return(indexlib::Status::IOError())));

    builder->start();
    while (!builder->_buildLoopStop) {
        usleep(10 * 1000); // 10ms
    }
    ASSERT_TRUE(builder->_needReload);
    ASSERT_EQ(2, builder->_totalBuildCount);
    ASSERT_TRUE(builder->_pipeline);

    builder->stop();
    ASSERT_FALSE(builder->_pipeline);
    ASSERT_FALSE(builder->_buildThread.joinable());
}

TEST_F(DirectBuilderTest, testAlterTable) {
    std::shared_ptr<indexlibv2::config::TabletSchema> schema(new indexlibv2::config::TabletSchema());
    schema->TEST_GetImpl()->SetSchemaId(1234);
    EXPECT_CALL(*_tablet, GetTabletSchema()).WillOnce(Return(schema));
    auto builder = makeMockBuilder();
    MockPipeline mp1;
    EXPECT_CALL(*builder, createReader(_))
        .WillOnce(Return(ByMove(std::unique_ptr<build_service::reader::RawDocumentReader>(mp1.mockReader))));
    EXPECT_CALL(*builder, createProcessor(_, _))
        .WillOnce(Return(ByMove(std::unique_ptr<build_service::processor::Processor>(mp1.mockProcessor))));
    EXPECT_CALL(*builder, createBuilder(_))
        .WillOnce(Return(ByMove(std::unique_ptr<build_service::builder::BuilderV2Impl>(mp1.mockBuilder))));
    builder->maybeInitBuildPipeline();
    ASSERT_TRUE(builder->_pipeline);
    string buildIdStr;
    _pid.buildid().SerializeToString(&buildIdStr);
    std::map<std::string, string> fieldMap = {{"config_path", _configRoot},
                                              {"schema_version", "1234"},
                                              {"CMD", "alter"},
                                              {"ha_reserved_timestamp", "1024"},
                                              {"build_id", buildIdStr}};
    auto unexpectedAlterDocMap = fieldMap;
    auto otherBuildId = _pid.buildid();
    string otherBuildIdStr;
    otherBuildId.set_generationid(10000);
    otherBuildId.SerializeToString(&otherBuildIdStr);
    unexpectedAlterDocMap[BUILD_ID_KEY] = otherBuildIdStr;
    EXPECT_CALL(*mp1.mockReader, read(_, _))
        .WillOnce(DoAll(doRead(fieldMap, 1, "swift"), Return(RawDocumentReader::ERROR_NONE)))
        .WillOnce(DoAll(doRead(fieldMap, 1, "swift"), Return(RawDocumentReader::ERROR_NONE)))
        .WillOnce(DoAll(doRead(fieldMap, 1, "swift"), Return(RawDocumentReader::ERROR_NONE)))
        .WillOnce(DoAll(doRead(unexpectedAlterDocMap, 1, "swift"), Return(RawDocumentReader::ERROR_NONE)))
        .WillOnce(DoAll(doRead(fieldMap, 1, "swift"), Return(RawDocumentReader::ERROR_NONE)));

    // load processor failed
    std::unique_ptr<build_service::processor::Processor> nullp;
    EXPECT_CALL(*builder, createProcessor(_, _)).WillOnce(Return(ByMove(std::move(nullp))));
    ASSERT_FALSE(builder->runBuildPipeline());
    ASSERT_EQ(mp1.mockProcessor, builder->_pipeline->processor.get());
    ASSERT_FALSE(builder->_needReload);

    // invalid argument
    auto p2 = new build_service::processor::MockProcessor();
    EXPECT_CALL(*builder, createProcessor(_, _))
        .WillOnce(Return(ByMove(std::unique_ptr<build_service::processor::Processor>(p2))));
    EXPECT_CALL(*_tablet, AlterTable(_)).WillOnce(Return(indexlib::Status::InvalidArgs()));
    ASSERT_FALSE(builder->runBuildPipeline());
    ASSERT_EQ(mp1.mockProcessor, builder->_pipeline->processor.get());
    ASSERT_FALSE(builder->_needReload);

    // fatal error
    auto p3 = new build_service::processor::MockProcessor();
    EXPECT_CALL(*builder, createProcessor(_, _))
        .WillOnce(Return(ByMove(std::unique_ptr<build_service::processor::Processor>(p3))));
    EXPECT_CALL(*_tablet, AlterTable(_)).WillOnce(Return(indexlib::Status::Corruption()));
    ASSERT_FALSE(builder->runBuildPipeline());
    ASSERT_EQ(mp1.mockProcessor, builder->_pipeline->processor.get());
    ASSERT_TRUE(builder->_needReload);

    // uninteresting alter table doc
    builder->_needReload = false;
    ASSERT_FALSE(builder->runBuildPipeline());
    ASSERT_EQ(mp1.mockProcessor, builder->_pipeline->processor.get());

    auto checkSchemaId = [&](const std::shared_ptr<indexlibv2::config::ITabletSchema> &schema) {
        ASSERT_EQ(1234, schema->GetSchemaId());
    };
    // success
    auto p5 = new build_service::processor::MockProcessor();
    EXPECT_CALL(*builder, createProcessor(_, _))
        .WillOnce(DoAll(Invoke(std::bind(checkSchemaId, std::placeholders::_2)),
                        Return(ByMove(std::unique_ptr<build_service::processor::Processor>(p5)))));
    EXPECT_CALL(*_tablet, AlterTable(_)).WillOnce(Return(indexlib::Status::OK()));
    ASSERT_FALSE(builder->runBuildPipeline());
    ASSERT_EQ(p5, builder->_pipeline->processor.get());
    ASSERT_FALSE(builder->_needReload);
}

TEST_F(DirectBuilderTest, testBulkload) {
    std::shared_ptr<indexlibv2::config::TabletSchema> schema(new indexlibv2::config::TabletSchema());
    schema->TEST_GetImpl()->SetSchemaId(1234);
    EXPECT_CALL(*_tablet, GetTabletSchema()).WillOnce(Return(schema));
    auto builder = makeMockBuilder();
    MockPipeline mp1;
    EXPECT_CALL(*builder, createReader(_))
        .WillOnce(Return(ByMove(std::unique_ptr<build_service::reader::RawDocumentReader>(mp1.mockReader))));
    EXPECT_CALL(*builder, createProcessor(_, _))
        .WillOnce(Return(ByMove(std::unique_ptr<build_service::processor::Processor>(mp1.mockProcessor))));
    EXPECT_CALL(*builder, createBuilder(_))
        .WillOnce(Return(ByMove(std::unique_ptr<build_service::builder::BuilderV2Impl>(mp1.mockBuilder))));
    builder->maybeInitBuildPipeline();
    ASSERT_TRUE(builder->_pipeline);
    string buildIdStr;
    _pid.buildid().SerializeToString(&buildIdStr);
    std::vector externalFiles = {"pangu://xx/1.sst", "pangu://xx/2.sst"};
    indexlibv2::framework::ImportExternalFileOptions options;
    std::map<std::string, string> fieldMap = {
        {BUILD_ID_KEY, autil::legacy::Base64EncodeFast(buildIdStr)},
        {EXTERNAL_FILES_KEY, autil::legacy::FastToJsonString(externalFiles)},
        {"CMD", "bulkload"},
        {BULKLOAD_ID_KEY, "123"},
        {IMPORT_EXTERNAL_FILE_OPTIONS_KEY, autil::legacy::FastToJsonString(options)}};
    auto unexpectedBulkloadDocMap = fieldMap;
    auto otherBuildId = _pid.buildid();
    string otherBuildIdStr;
    otherBuildId.set_generationid(10000);
    otherBuildId.SerializeToString(&otherBuildIdStr);
    unexpectedBulkloadDocMap[BUILD_ID_KEY] = autil::legacy::Base64EncodeFast(otherBuildIdStr);
    EXPECT_CALL(*mp1.mockReader, read(_, _))
        .WillOnce(DoAll(doRead(unexpectedBulkloadDocMap, 1, "swift"), Return(RawDocumentReader::ERROR_NONE)))
        .WillOnce(DoAll(doRead(fieldMap, 1, "swift"), Return(RawDocumentReader::ERROR_NONE)))
        .WillOnce(DoAll(doRead(fieldMap, 1, "swift"), Return(RawDocumentReader::ERROR_NONE)))
        .WillOnce(DoAll(doRead(fieldMap, 1, "swift"), Return(RawDocumentReader::ERROR_NONE)));
    ASSERT_FALSE(builder->runBuildPipeline());
    ASSERT_FALSE(builder->_needReload);

    // invalid args
    EXPECT_CALL(*_tablet, ImportExternalFiles(_, _, _, _)).WillOnce(Return(indexlib::Status::InvalidArgs()));
    ASSERT_FALSE(builder->runBuildPipeline());
    ASSERT_FALSE(builder->_needReload);
    builder->_needReload = false;

    // fatal error
    EXPECT_CALL(*_tablet, ImportExternalFiles(_, _, _, _)).WillOnce(Return(indexlib::Status::Corruption()));
    ASSERT_FALSE(builder->runBuildPipeline());
    ASSERT_TRUE(builder->_needReload);
    builder->_needReload = false;

    // success
    EXPECT_CALL(*_tablet, ImportExternalFiles(_, _, _, _)).WillOnce(Return(indexlib::Status::OK()));
    ASSERT_FALSE(builder->runBuildPipeline());
    ASSERT_FALSE(builder->_needReload);
    builder->_needReload = false;
}

using build_service::reader::SwiftRawDocumentReader;

class MockSwiftRawDocumentReader : public SwiftRawDocumentReader {
public:
    MockSwiftRawDocumentReader(const build_service::util::SwiftClientCreatorPtr &swiftClientCreator)
        : SwiftRawDocumentReader(swiftClientCreator) {}

public:
    MOCK_METHOD1(getMaxTimestamp, bool(int64_t &));
};
using NiceMockSwiftRawDocumentReader = ::testing::NiceMock<MockSwiftRawDocumentReader>;

TEST_F(DirectBuilderTest, testRecoverByMaxRecoverTime) {
    MockPipeline mp;
    auto builder = makeBuilderWithMockPipeline(mp);
    ASSERT_FALSE(builder->_isRecovered);
    ASSERT_EQ(DirectBuilder::DEFAULT_MAX_RECOVER_TIME_IN_SEC, builder->_maxRecoverTimeInSec);
    ASSERT_EQ(DirectBuilder::DEFAULT_BUILD_DELAY_IN_US, builder->_buildDelayInUs);
    ASSERT_EQ(0, builder->_startRecoverTimeInSec);
    builder->initRecoverParameters();
    ASSERT_EQ(1, builder->_maxRecoverTimeInSec);
    ASSERT_EQ(DirectBuilder::DEFAULT_BUILD_DELAY_IN_US, builder->_buildDelayInUs);
    ASSERT_GT(builder->_startRecoverTimeInSec, 0);
    sleep(1);
    ASSERT_TRUE(builder->isRecovered());
    ASSERT_TRUE(builder->_isRecovered);
}

TEST_F(DirectBuilderTest, testRecoverByBuildDelay) {
    MockPipeline mp;
    auto builder = makeBuilderWithMockPipeline(mp);
    builder->initRecoverParameters();
    ASSERT_EQ(DirectBuilder::DEFAULT_MAX_RECOVER_TIME_IN_SEC, builder->_maxRecoverTimeInSec);
    builder->_maxRecoverTimeInSec = 100;
    auto swiftReader = new NiceMockSwiftRawDocumentReader(_builderResource->swiftClientCreator);
    builder->_pipeline->reader.reset(swiftReader);
    ASSERT_FALSE(builder->_isRecovered);
    ASSERT_EQ(DirectBuilder::DEFAULT_BUILD_DELAY_IN_US, builder->_buildDelayInUs);
    builder->_buildDelayInUs = 1; // 1us

    indexlibv2::framework::TabletInfos tabletInfos;
    EXPECT_CALL(*_tablet, GetTabletInfos()).WillRepeatedly(Return(&tabletInfos));

    EXPECT_CALL(*swiftReader, getMaxTimestamp(_)).WillOnce(Return(false));
    ASSERT_FALSE(builder->isRecovered());

    // locator invalid
    int64_t maxTimestamp = 1234;
    EXPECT_CALL(*swiftReader, getMaxTimestamp(_)).WillOnce(DoAll(SetArgReferee<0>(maxTimestamp), Return(true)));
    ASSERT_FALSE(builder->isRecovered());

    // locator src not equal
    EXPECT_CALL(*swiftReader, getMaxTimestamp(_)).WillOnce(DoAll(SetArgReferee<0>(maxTimestamp), Return(true)));
    tabletInfos.SetBuildLocator(indexlibv2::framework::Locator(0, 1230));
    ASSERT_FALSE(builder->isRecovered());

    // buildDelay too large
    EXPECT_CALL(*swiftReader, getMaxTimestamp(_)).WillOnce(DoAll(SetArgReferee<0>(maxTimestamp), Return(true)));
    tabletInfos.SetBuildLocator(indexlibv2::framework::Locator(1, 1230));
    ASSERT_FALSE(builder->isRecovered());

    // buildDelay is close enough
    EXPECT_CALL(*swiftReader, getMaxTimestamp(_)).WillOnce(DoAll(SetArgReferee<0>(maxTimestamp), Return(true)));
    tabletInfos.SetBuildLocator(indexlibv2::framework::Locator(1, 1233));
    ASSERT_TRUE(builder->isRecovered());
    ASSERT_TRUE(builder->_isRecovered);
}

} // namespace suez
