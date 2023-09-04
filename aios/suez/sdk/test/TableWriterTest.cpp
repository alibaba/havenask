#include "suez/sdk/TableWriter.h"

#include "autil/result/Errors.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/reader/DocumentSeparators.h"
#include "build_service/reader/RawDocumentBuilder.h"
#include "build_service/reader/SwiftFieldFilterRawDocumentParser.h"
#include "build_service/util/SwiftClientCreator.h"
#include "indexlib/document/document_factory_wrapper.h"
#include "indexlib/document/raw_document/raw_document_define.h"
#include "indexlib/document/raw_document_parser.h"
#include "indexlib/framework/TabletInfos.h"
#include "indexlib/framework/mock/MockTablet.h"
#include "kmonitor/client/MetricsReporter.h"
#include "suez/table/test/MockWALStrategy.h"
#include "suez/table/wal/CommonDefine.h"
#include "suez/table/wal/WALConfig.h"
#include "suez/table/wal/WALStrategy.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;

namespace suez {

class TableWriterTest : public TESTBASE {
public:
    void setUp() override {
        _configRoot = GET_TEST_DATA_PATH() + "table_test/table_writer/config/0";
        initPid();
        _swiftClientCreator = std::make_shared<build_service::util::SwiftClientCreator>();
        kmonitor::MetricsTags metricsTags;
        _metricsReporter = std::make_shared<kmonitor::MetricsReporter>("", metricsTags, "");

        _format = build_service::reader::RAW_DOCUMENT_HA3_DOCUMENT_FORMAT;
        _tableWriter = std::make_unique<TableWriter>();
    }

    void tearDown() override { _pid.Clear(); }

protected:
    std::string makeHa3FormatDoc(const std::map<std::string, std::string> &fields) const {
        build_service::reader::RawDocumentBuilder builder(build_service::reader::RAW_DOCUMENT_HA3_SEP_PREFIX,
                                                          build_service::reader::RAW_DOCUMENT_HA3_SEP_SUFFIX,
                                                          build_service::reader::RAW_DOCUMENT_HA3_FIELD_SEP,
                                                          build_service::reader::RAW_DOCUMENT_HA3_KV_SEP);
        for (const auto &it : fields) {
            builder.addField(autil::StringView(it.first), it.second);
        }
        return builder.finalize().to_string();
    }

    bool initTableWriter(const WALConfig &walConfig = WALConfig()) {
        bool ret = _tableWriter->init(_pid, _configRoot, _metricsReporter, walConfig, _swiftClientCreator);
        if (ret) {
            _tableWriter->setEnableWrite(true);
        }
        return ret;
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
    std::string _configRoot;
    build_service::proto::PartitionId _pid;
    std::shared_ptr<build_service::util::SwiftClientCreator> _swiftClientCreator;
    std::shared_ptr<kmonitor::MetricsReporter> _metricsReporter;
    std::string _format;
    std::unique_ptr<TableWriter> _tableWriter;
};

TEST_F(TableWriterTest, testInitSimple) {
    WALConfig walConfig;
    ASSERT_TRUE(_tableWriter->init(_pid, _configRoot, _metricsReporter, walConfig, _swiftClientCreator));
    ASSERT_TRUE(_tableWriter->_documentFactoryWrapper);
    ASSERT_TRUE(_tableWriter->_wal);
}

TEST_F(TableWriterTest, testInitWithWalFail) {
    WALConfig walConfig;
    walConfig.strategy = "realtime_swift";
    ASSERT_TRUE(_tableWriter->init(_pid, _configRoot, _metricsReporter, walConfig, _swiftClientCreator));
    ASSERT_TRUE(_tableWriter->_documentFactoryWrapper);
    ASSERT_FALSE(_tableWriter->_wal);
}

TEST_F(TableWriterTest, testParseWalDocsFail) {
    ASSERT_TRUE(initTableWriter());

    WalDocVector docs = {{1, "xxx"}};
    ASSERT_FALSE(_tableWriter->parseWalDocs("unknown", docs).is_ok());
    ASSERT_FALSE(_tableWriter->parseWalDocs(_format, docs).is_ok());
}

TEST_F(TableWriterTest, testParseWalDocs) {
    ASSERT_TRUE(initTableWriter());

    {
        // empty
        auto result = _tableWriter->parseWalDocs(_format, {});
        ASSERT_TRUE(result.is_ok());
        auto walDocs = std::move(result).steal_value();
        ASSERT_TRUE(walDocs.empty());
    }

    WalDocVector docs = {{1, makeHa3FormatDoc({{"f1", "v11"}, {"f2", "v21"}})},
                         {2, makeHa3FormatDoc({{"f1", "v12"}, {"f2", "v22"}})}};
    auto result = _tableWriter->parseWalDocs(_format, docs);
    ASSERT_TRUE(result.is_ok());
    auto walDocs = std::move(result).steal_value();
    ASSERT_EQ(2, walDocs.size());
    ASSERT_EQ(1, walDocs[0].first);
    ASSERT_EQ(2, walDocs[1].first);
    // TODO: check doc content
}

TEST_F(TableWriterTest, testStop) {
    ASSERT_TRUE(initTableWriter());
    ASSERT_TRUE(_tableWriter->_enableWrite);
    ASSERT_TRUE(_tableWriter->_wal);
    _tableWriter->stop();
    ASSERT_FALSE(_tableWriter->_enableWrite);
    ASSERT_FALSE(_tableWriter->_wal);
}

TEST_F(TableWriterTest, testStop2) {
    ASSERT_TRUE(initTableWriter());
    auto wal = std::make_unique<NiceMockWALStrategy>();
    auto mockWal = wal.get();
    _tableWriter->_wal = std::move(wal);

    EXPECT_CALL(*mockWal, flush()).Times(1);
    EXPECT_CALL(*mockWal, stop()).Times(1);
    _tableWriter->stop();
    ASSERT_FALSE(_tableWriter->_enableWrite);
    ASSERT_FALSE(_tableWriter->_wal);
}

TEST_F(TableWriterTest, testSetEnableWrite) {
    ASSERT_TRUE(initTableWriter());
    auto wal = std::make_unique<NiceMockWALStrategy>();
    auto mockWal = wal.get();
    _tableWriter->_wal = std::move(wal);

    EXPECT_CALL(*mockWal, flush()).Times(0);
    _tableWriter->setEnableWrite(true);
    ASSERT_TRUE(_tableWriter->_enableWrite);

    EXPECT_CALL(*mockWal, flush());
    _tableWriter->setEnableWrite(false);
    ASSERT_FALSE(_tableWriter->_enableWrite);

    EXPECT_CALL(*mockWal, flush()).Times(0);
    _tableWriter->setEnableWrite(true);
    ASSERT_TRUE(_tableWriter->_enableWrite);

    EXPECT_CALL(*mockWal, flush());
    EXPECT_CALL(*mockWal, stop());
    _tableWriter->stop();
    ASSERT_FALSE(_tableWriter->_enableWrite);
}

ACTION_P2(CallLogDone, result, message, future) {
    autil::Result<std::vector<int64_t>> ret;
    std::string error(message);
    if (!error.empty()) {
        ret = autil::result::RuntimeError::make("%s", error.c_str());
    } else {
        ret = result;
    }
    *future = std::move(std::async(std::launch::async, arg1, std::move(ret)));
}

TEST_F(TableWriterTest, testWriteFailed) {
    ASSERT_TRUE(initTableWriter());

    // disable write
    _tableWriter->_enableWrite = false;
    bool callDone = false;
    auto done1 = [&callDone](autil::Result<WriteResult> result) {
        ASSERT_FALSE(result.is_ok());
        callDone = true;
    };
    _tableWriter->write(_format, {}, std::move(done1));
    ASSERT_TRUE(callDone);

    // doc invalid
    callDone = false;
    _tableWriter->_enableWrite = true;
    auto done2 = [&callDone](autil::Result<WriteResult> result) {
        ASSERT_FALSE(result.is_ok());
        callDone = true;
    };
    WalDocVector docs = {{1, "xx"}};
    _tableWriter->write("xx", docs, std::move(done2));
    ASSERT_TRUE(callDone);

    // log failed
    callDone = false;
    auto wal = std::make_unique<NiceMockWALStrategy>();
    auto mockWal = wal.get();
    _tableWriter->_wal = std::move(wal);
    auto done3 = [&callDone](autil::Result<WriteResult> result) {
        ASSERT_FALSE(result.is_ok());
        ASSERT_EQ("topic not exist", result.get_error().message());
        callDone = true;
    };
    std::future<void> logDone;
    EXPECT_CALL(*mockWal, log(_, _)).WillOnce(CallLogDone(std::vector<int64_t>{}, "topic not exist", &logDone));
    docs = {{1, makeHa3FormatDoc({{"f1", "v11"}, {"f2", "v21"}})},
            {2, makeHa3FormatDoc({{"f1", "v12"}, {"f2", "v22"}})}};
    _tableWriter->write(_format, docs, std::move(done3));
    logDone.get();
    ASSERT_TRUE(callDone);
}

TEST_F(TableWriterTest, testWriteEmpty) {
    ASSERT_TRUE(initTableWriter());
    auto wal = std::make_unique<NiceMockWALStrategy>();
    auto mockWal = wal.get();
    _tableWriter->_wal = std::move(wal);

    bool callDone = false;
    auto done = [&callDone](autil::Result<WriteResult> result) {
        ASSERT_TRUE(result.is_ok());
        callDone = true;
    };
    EXPECT_CALL(*mockWal, log(_, _)).Times(0);
    _tableWriter->write(_format, {}, std::move(done));
    ASSERT_TRUE(callDone);
}

TEST_F(TableWriterTest, testWriteLog) {
    ASSERT_TRUE(initTableWriter());
    auto wal = std::make_unique<NiceMockWALStrategy>();
    auto mockWal = wal.get();
    _tableWriter->_wal = std::move(wal);

    bool callDone = false;
    auto done1 = [&callDone](autil::Result<WriteResult> result) {
        ASSERT_TRUE(result.is_ok());
        auto wr = std::move(result).steal_value();
        ASSERT_EQ(WriterState::LOG, wr.state);
        ASSERT_EQ(2, wr.stat.inMessageCount);
        ASSERT_EQ(2, wr.watermark.maxCp);
        ASSERT_EQ(std::numeric_limits<int64_t>::min(), wr.watermark.buildGap);
        ASSERT_EQ(-1, wr.watermark.buildLocatorOffset);
        ASSERT_TRUE(wr.stat.parseLatency > 0);
        ASSERT_TRUE(wr.stat.logLatency > 0);
        callDone = true;
    };

    std::future<void> logDone;
    EXPECT_CALL(*mockWal, log(_, _)).WillOnce(CallLogDone(std::vector<int64_t>{1, 2}, "", &logDone));
    WalDocVector docs = {{1, makeHa3FormatDoc({{"f1", "v11"}, {"f2", "v21"}})},
                         {2, makeHa3FormatDoc({{"f1", "v12"}, {"f2", "v22"}})}};
    _tableWriter->write(_format, docs, std::move(done1));
    logDone.get();
    ASSERT_TRUE(callDone);
    ASSERT_EQ(2, _tableWriter->getLatestLogOffset());
}

TEST_F(TableWriterTest, testWriteAsync) {
    ASSERT_TRUE(initTableWriter());
    auto wal = std::make_unique<NiceMockWALStrategy>();
    auto mockWal = wal.get();
    _tableWriter->_wal = std::move(wal);

    auto tablet = std::make_shared<indexlibv2::framework::NiceMockTablet>();
    _tableWriter->updateIndex(tablet);

    indexlibv2::framework::TabletInfos tabletInfos;
    tabletInfos.SetBuildLocator(indexlibv2::framework::Locator(1, 1));
    EXPECT_CALL(*tablet, GetTabletInfos()).WillOnce(Return(&tabletInfos));

    bool callDone = false;
    auto done1 = [&callDone](autil::Result<WriteResult> result) {
        ASSERT_TRUE(result.is_ok());
        auto wr = std::move(result).steal_value();
        ASSERT_EQ(WriterState::ASYNC, wr.state);
        ASSERT_EQ(2, wr.stat.inMessageCount);
        ASSERT_EQ(2, wr.watermark.maxCp);
        ASSERT_EQ(1, wr.watermark.buildGap);
        ASSERT_EQ(1, wr.watermark.buildLocatorOffset);
        ASSERT_TRUE(wr.stat.parseLatency > 0);
        ASSERT_TRUE(wr.stat.logLatency > 0);
        callDone = true;
    };

    std::future<void> logDone;
    EXPECT_CALL(*mockWal, log(_, _)).WillOnce(CallLogDone(std::vector<int64_t>{1, 2}, "", &logDone));
    WalDocVector docs = {{1, makeHa3FormatDoc({{"f1", "v11"}, {"f2", "v21"}})},
                         {2, makeHa3FormatDoc({{"f1", "v12"}, {"f2", "v22"}})}};
    _tableWriter->write(_format, docs, std::move(done1));
    logDone.get();
    ASSERT_TRUE(callDone);
    ASSERT_EQ(2, _tableWriter->getLatestLogOffset());
}

TEST_F(TableWriterTest, testAlterTableDoc) {
    ASSERT_TRUE(initTableWriter());
    auto wal = std::make_unique<NiceMockWALStrategy>();
    auto mockWal = wal.get();
    _tableWriter->_wal = std::move(wal);

    auto tablet = std::make_shared<indexlibv2::framework::NiceMockTablet>();
    _tableWriter->updateIndex(tablet);

    bool callDone = false;
    auto done1 = [&callDone](autil::Result<int64_t> result) {
        ASSERT_TRUE(result.is_ok());
        callDone = true;
    };
    string configPath = "test/schema.json";
    uint32_t version = 123;
    string buildIdStr;
    _pid.buildid().SerializeToString(&buildIdStr);
    auto compareFunc = [&](const WalDocVector &walDoc) {
        ASSERT_EQ(1, walDoc.size());
        auto doc = walDoc[0];
        indexlib::document::RawDocumentPtr rawDoc(_tableWriter->_documentFactoryWrapper->CreateRawDocument());
        auto parser = _tableWriter->getParser(build_service::reader::RAW_DOCUMENT_FORMAT_SWIFT_FILED_FILTER);
        ASSERT_TRUE(parser);
        ASSERT_TRUE(parser->parse(doc.second, *rawDoc));
        ASSERT_EQ(configPath, rawDoc->getField(CONFIG_PATH_KEY));
        ASSERT_EQ(std::to_string(version), rawDoc->getField(SCHEMA_VERSION_KEY));
        ASSERT_EQ(buildIdStr, rawDoc->getField(BUILD_ID_KEY));
    };

    std::future<void> logDone;
    EXPECT_CALL(*mockWal, log(_, _))
        .WillOnce(DoAll(Invoke(std::bind(compareFunc, std::placeholders::_1)),
                        CallLogDone(std::vector<int64_t>{3}, "", &logDone)));
    _tableWriter->updateSchema(
        /*schema version*/ version, /*config path*/ configPath, done1);
    logDone.get();
    ASSERT_TRUE(callDone);
    ASSERT_EQ(3, _tableWriter->getLatestLogOffset());
}

} // namespace suez
