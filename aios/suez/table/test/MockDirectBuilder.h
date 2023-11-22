#pragma once

#include "build_service/config/ResourceReader.h"
#include "suez/table/DirectBuilder.h"
#include "suez/table/wal/WALConfig.h"
#include "unittest/unittest.h"

namespace suez {

class MockDirectBuilder : public DirectBuilder {
public:
    MockDirectBuilder(const build_service::proto::PartitionId &pid,
                      const std::shared_ptr<indexlibv2::framework::ITablet> &tablet,
                      const build_service::workflow::RealtimeBuilderResource &rtResource,
                      const std::string &configPath,
                      const WALConfig &walConfig)
        : DirectBuilder(pid, tablet, rtResource, configPath, walConfig) {}

public:
    MOCK_CONST_METHOD1(
        createReader,
        std::unique_ptr<build_service::reader::RawDocumentReader>(const build_service::config::ResourceReaderPtr &));
    MOCK_CONST_METHOD2(createProcessor,
                       std::unique_ptr<build_service::processor::Processor>(
                           const build_service::config::ResourceReaderPtr &,
                           const std::shared_ptr<indexlibv2::config::ITabletSchema> &schema));
    MOCK_CONST_METHOD1(
        createBuilder,
        std::unique_ptr<build_service::builder::BuilderV2Impl>(const build_service::config::ResourceReaderPtr &));
    MOCK_CONST_METHOD0(createRawDocRewriter, std::unique_ptr<build_service::workflow::RawDocumentRewriter>());
};
using NiceMockDirectBuilder = ::testing::NiceMock<MockDirectBuilder>;

} // namespace suez
