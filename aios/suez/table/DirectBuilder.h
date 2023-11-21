/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <atomic>
#include <memory>
#include <optional>
#include <thread>

#include "autil/Lock.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "suez/table/TableBuilder.h"

namespace build_service {

namespace builder {
class BuilderV2Impl;
} // namespace builder

namespace config {
class ResourceReader;
} // namespace config

namespace processor {
class Processor;
} // namespace processor

namespace reader {
class RawDocumentReaderCreator;
class RawDocumentReader;
struct Checkpoint;
} // namespace reader

namespace workflow {
struct RealtimeBuilderResource;
class RawDocumentRewriter;
} // namespace workflow

} // namespace build_service

namespace indexlibv2::framework {
class ITablet;
struct ImportExternalFileOptions;
} // namespace indexlibv2::framework

namespace indexlibv2::config {
class ITabletSchema;
}

namespace indexlibv2::document {
class IDocumentBatch;
class RawDocument;
} // namespace indexlibv2::document

namespace indexlib::util {
class MetricProvider;
}

namespace suez {

class RawDocumentReaderCreatorAdapter;
class WALConfig;

class DirectBuilder : public TableBuilder {
private:
    struct Pipeline {
        Pipeline();
        ~Pipeline();

        void stop();

        std::unique_ptr<build_service::reader::RawDocumentReader> reader;
        std::unique_ptr<build_service::workflow::RawDocumentRewriter> rewriter;
        std::unique_ptr<build_service::processor::Processor> processor;
        std::unique_ptr<build_service::builder::BuilderV2Impl> builder;
    };

public:
    DirectBuilder(const build_service::proto::PartitionId &pid,
                  const std::shared_ptr<indexlibv2::framework::ITablet> &tablet,
                  const build_service::workflow::RealtimeBuilderResource &rtResource,
                  const std::string &configPath,
                  const WALConfig &walConfig);
    virtual ~DirectBuilder();

public:
    bool start() override;
    void stop() override;
    void suspend() override;
    void resume() override;
    void skip(int64_t timestamp) override;
    void forceRecover() override;
    bool isRecovered() override;
    bool needCommit() const override;
    std::pair<bool, TableVersion> commit() override;

public:
    void TEST_setLastLocator(const std::shared_ptr<indexlibv2::framework::Locator> &locator) {
        setLastLocator(locator);
    }

private:
    void initRecoverParameters();
    void buildWorkLoop();
    bool runBuildPipeline();
    bool buildWithRetry(build_service::builder::BuilderV2Impl *builder,
                        const std::shared_ptr<indexlibv2::document::IDocumentBatch> &batch);
    bool rewriteWithRetry(build_service::workflow::RawDocumentRewriter *rewriter,
                          const std::shared_ptr<indexlibv2::document::RawDocument> &rawDoc);
    void maybeInitBuildPipeline();

    // virtual for test
    virtual std::unique_ptr<build_service::reader::RawDocumentReader>
    createReader(const std::shared_ptr<build_service::config::ResourceReader> &resourceReader) const;
    virtual std::unique_ptr<build_service::processor::Processor>
    createProcessor(const std::shared_ptr<build_service::config::ResourceReader> &resourceReader,
                    const std::shared_ptr<indexlibv2::config::ITabletSchema> &schema) const;
    virtual std::unique_ptr<build_service::builder::BuilderV2Impl>
    createBuilder(const std::shared_ptr<build_service::config::ResourceReader> &resourceReader) const;
    virtual std::unique_ptr<build_service::workflow::RawDocumentRewriter> createRawDocRewriter() const;

    bool alterTable(uint32_t version, const std::string &configPath);
    bool processAlterTableDoc(const std::shared_ptr<indexlibv2::document::RawDocument> &rawDoc);
    bool processBulkloadDoc(const std::shared_ptr<indexlibv2::document::RawDocument> &rawDoc);
    void setLastLocator(const std::shared_ptr<indexlibv2::framework::Locator> &locator);

private:
    build_service::proto::PartitionId _pid;
    // resource for create reader/processor/builder
    std::shared_ptr<indexlibv2::framework::ITablet> _tablet;
    std::unique_ptr<build_service::workflow::RealtimeBuilderResource> _rtBuilderResource;
    std::shared_ptr<build_service::config::ResourceReader> _resourceReader;
    std::unique_ptr<RawDocumentReaderCreatorAdapter> _readerCreatorAdapter;
    std::optional<uint64_t> _srcSignature;

    std::unique_ptr<Pipeline> _pipeline;
    mutable autil::ThreadMutex _mutex;
    std::atomic<bool> _buildLoopStop = false;
    std::atomic<bool> _needReload = false;
    std::thread _buildThread;
    std::atomic<size_t> _totalBuildCount = 0;
    std::shared_ptr<indexlibv2::framework::Locator> _lastLocator;

    // recover control parameters
    int64_t _maxRecoverTimeInSec = DEFAULT_MAX_RECOVER_TIME_IN_SEC;
    int64_t _buildDelayInUs = DEFAULT_BUILD_DELAY_IN_US;
    int64_t _startRecoverTimeInSec = 0;
    std::atomic<int64_t> _isRecovered = false;

private:
    static constexpr int64_t DEFAULT_MAX_RECOVER_TIME_IN_SEC = 1;         // 1s
    static constexpr int64_t DEFAULT_BUILD_DELAY_IN_US = 1 * 1000 * 1000; // 1s
};

} // namespace suez
