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
#include "suez/table/DirectBuilder.h"

#include "autil/Log.h"
#include "autil/Scope.h"
#include "autil/ThreadNameScope.h"
#include "autil/legacy/base64.h"
#include "build_service/build_task/BuildTask.h"
#include "build_service/builder/BuilderV2Impl.h"
#include "build_service/common/ConfigDownloader.h"
#include "build_service/config/BuilderClusterConfig.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/processor/Processor.h"
#include "build_service/proto/ProtoComparator.h"
#include "build_service/reader/RawDocumentReader.h"
#include "build_service/reader/RawDocumentReaderCreator.h"
#include "build_service/reader/SwiftRawDocumentReader.h"
#include "build_service/util/LocatorUtil.h"
#include "build_service/workflow/RawDocumentRewriter.h"
#include "build_service/workflow/RealtimeBuilderDefine.h"
#include "fslib/util/FileUtil.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/document/DocumentBatch.h"
#include "indexlib/framework/ITablet.h"
#include "indexlib/framework/TabletInfos.h"
#include "indexlib/framework/index_task/Constant.h"
#include "indexlib/framework/index_task/MergeTaskDefine.h"
#include "suez/common/InnerDef.h"
#include "suez/common/TablePathDefine.h"
#include "suez/table/RawDocumentReaderCreatorAdapter.h"
#include "suez/table/wal/CommonDefine.h"
#include "suez/table/wal/WALConfig.h"

namespace suez {
AUTIL_DECLARE_AND_SETUP_LOGGER(suez, DirectBuilder);

using BuildTask = build_service::build_task::BuildTask;

DirectBuilder::Pipeline::Pipeline() {}

DirectBuilder::Pipeline::~Pipeline() {}

void DirectBuilder::Pipeline::stop() {
    if (processor) {
        processor->stop(true, false);
        processor.reset();
    }
    if (builder) {
        builder->stop(std::nullopt, false, true);
        builder.reset();
    }
    reader.reset();
}

DirectBuilder::DirectBuilder(const build_service::proto::PartitionId &pid,
                             const std::shared_ptr<indexlibv2::framework::ITablet> &tablet,
                             const build_service::workflow::RealtimeBuilderResource &rtResource,
                             const std::string &configPath,
                             const WALConfig &walConfig)
    : _pid(pid)
    , _tablet(tablet)
    , _rtBuilderResource(std::make_unique<build_service::workflow::RealtimeBuilderResource>(rtResource))
    , _resourceReader(std::make_shared<build_service::config::ResourceReader>(configPath))
    , _readerCreatorAdapter(
          std::make_unique<RawDocumentReaderCreatorAdapter>(rtResource.swiftClientCreator, walConfig)) {
    auto srcSignature = _rtBuilderResource->realtimeInfo.getSrcSignature();
    uint64_t src = 0;
    if (srcSignature.Get(&src)) {
        _srcSignature = src;
    }
}

DirectBuilder::~DirectBuilder() { stop(); }

#define OP_LOG(op)                                                                                                     \
    AUTIL_LOG(INFO, "[%p,%s] %s begin", this, _pid.ShortDebugString().c_str(), op);                                    \
    autil::ScopedTime2 opTime;                                                                                         \
    auto end = [&]() {                                                                                                 \
        auto latency = opTime.done_us();                                                                               \
        AUTIL_LOG(INFO, "[%p,%s] %s end, used time: %ld us", this, _pid.ShortDebugString().c_str(), op, latency);      \
    };                                                                                                                 \
    autil::ScopeGuard logGuard(std::move(end));

bool DirectBuilder::start() {
    initRecoverParameters();

    _buildLoopStop = false;
    _buildThread = std::thread([this]() {
        autil::ThreadNameScope _("DirectBuilder");
        buildWorkLoop();
    });
    return true;
}

void DirectBuilder::stop() {
    _buildLoopStop = true;
    if (_buildThread.joinable()) {
        _buildThread.join();
    }
    autil::ScopedLock lock(_mutex);
    if (_pipeline) {
        _pipeline->stop();
        _pipeline.reset();
    }
}

void DirectBuilder::suspend() {}

void DirectBuilder::resume() {}

void DirectBuilder::skip(int64_t timestamp) {}

void DirectBuilder::forceRecover() { _isRecovered.store(true, std::memory_order_relaxed); }

bool DirectBuilder::isRecovered() {
    static constexpr int64_t SLEEP_TIME = 10 * 1000; // 10ms
    if (_buildLoopStop) {
        // 避免卡住其他线程
        return true;
    }
    if (_isRecovered) {
        return true;
    }
    // check recover time
    auto now = autil::TimeUtility::currentTimeInSeconds();
    if (now >= _startRecoverTimeInSec + _maxRecoverTimeInSec) {
        _isRecovered = true;
        return true;
    }
    autil::ScopedLock lock(_mutex);
    if (!_pipeline) {
        usleep(SLEEP_TIME);
        return false;
    }
    auto swiftReader = dynamic_cast<build_service::reader::SwiftRawDocumentReader *>(_pipeline->reader.get());
    if (!swiftReader) {
        AUTIL_LOG(ERROR, "%s: reader is not swift source", _pid.ShortDebugString().c_str());
        return true;
    }
    int64_t maxTimestamp;
    if (!swiftReader->getMaxTimestamp(maxTimestamp)) {
        usleep(SLEEP_TIME);
        return false;
    }
    if (maxTimestamp == -1) {
        // swift no data
        _isRecovered = true;
        return true;
    }
    if (!_lastLocator || !(_lastLocator->IsValid()) ||
        (_srcSignature && _srcSignature.value() != _lastLocator->GetSrc()) ||
        build_service::util::LocatorUtil::getSwiftWatermark(*_lastLocator) + _buildDelayInUs < maxTimestamp) {
        usleep(SLEEP_TIME);
        return false;
    }
    _isRecovered = true;
    return true;
}

bool DirectBuilder::needCommit() const {
    if (!_allowCommit) {
        return false;
    }
    if (_needReload) {
        return true;
    }
    return _tablet->NeedCommit();
}

std::pair<bool, TableVersion> DirectBuilder::commit() {
    AUTIL_LOG(INFO, "%s begin commit", _pid.ShortDebugString().c_str());
    if (_needReload) {
        return {false, TableVersion()};
    }

    assert(_tablet->GetTabletOptions());
    bool needReopen = _tablet->GetTabletOptions()->FlushLocal();
    auto commitOptions = indexlibv2::framework::CommitOptions().SetNeedPublish(true).SetNeedReopenInCommit(needReopen);
    commitOptions.AddVersionDescription("generation", autil::StringUtil::toString(_pid.buildid().generationid()));
    auto [s, versionMeta] = _tablet->Commit(commitOptions);
    AUTIL_LOG(INFO, "%s finish commit", _pid.DebugString().c_str());

    TableVersion version;
    if (s.IsOK()) {
        version.setVersionId(versionMeta.GetVersionId());
        version.setVersionMeta(versionMeta);
        version.setFinished(versionMeta.IsSealed());
    }
    return {s.IsOK(), version};
}

void DirectBuilder::initRecoverParameters() {
    int64_t maxRecoverTimeInSeconds = 0;
    auto relativePath = build_service::config::ResourceReader::getClusterConfRelativePath(_pid.clusternames(0));
    if (_resourceReader->getConfigWithJsonPath(
            relativePath, "build_option_config.max_recover_time", maxRecoverTimeInSeconds) &&
        maxRecoverTimeInSeconds > 0) {
        _maxRecoverTimeInSec = maxRecoverTimeInSeconds;
    }

    int64_t buildDelayInMs = 0;
    if (_resourceReader->getConfigWithJsonPath(
            relativePath, "build_option_config.recover_delay_tolerance_ms", buildDelayInMs) &&
        buildDelayInMs > 0) {
        _buildDelayInUs = buildDelayInMs * 1000;
    }
    _startRecoverTimeInSec = autil::TimeUtility::currentTimeInSeconds();

    AUTIL_LOG(INFO,
              "%s: startRecoverTimeInSec=%ld, maxRecoverTimeInSec=%ld, buildDelayInUs=%ld",
              _pid.ShortDebugString().c_str(),
              _startRecoverTimeInSec,
              _maxRecoverTimeInSec,
              _buildDelayInUs);
}

void DirectBuilder::buildWorkLoop() {
    OP_LOG("buildWorkLoop");
    while (!_buildLoopStop) {
        maybeInitBuildPipeline();
        if (!_pipeline) {
            usleep(5 * 1000); // wait 5s
            continue;
        }
        if (runBuildPipeline()) {
            _totalBuildCount.fetch_add(1, std::memory_order_relaxed);
        }
        if (_needReload) {
            AUTIL_LOG(ERROR, "fatal error happens, need reload, stop build");
            _buildLoopStop = true;
            break;
        }
    }
}

bool DirectBuilder::runBuildPipeline() {
    // read
    auto reader = _pipeline->reader.get();
    build_service::document::RawDocumentPtr rawDoc;
    build_service::reader::Checkpoint ckpt;
    auto ec = reader->read(rawDoc, &ckpt);
    if (build_service::reader::RawDocumentReader::ERROR_NONE != ec) {
        if (build_service::reader::RawDocumentReader::ERROR_WAIT != ec) {
            AUTIL_LOG(ERROR, "%s: read failed, error code: %d", _pid.ShortDebugString().c_str(), (int)ec);
        }
        usleep(1 * 1000);
        return false;
    }
    // update read locator
    auto locator = std::make_shared<indexlibv2::framework::Locator>();
    locator->SetMultiProgress(ckpt.progress);
    locator->SetUserData(ckpt.userData);
    if (_srcSignature) {
        locator->SetSrc(_srcSignature.value());
    }
    setLastLocator(locator);

    if (rawDoc->getDocOperateType() == ALTER_DOC) {
        return processAlterTableDoc(rawDoc);
    } else if (rawDoc->getDocOperateType() == BULKLOAD_DOC) {
        return processBulkloadDoc(rawDoc);
    }

    // rewrite
    auto rewriter = _pipeline->rewriter.get();
    if (!rewriteWithRetry(rewriter, rawDoc)) {
        AUTIL_LOG(ERROR,
                  "%s: rewrite with retry failed at rawDoc: %s, will stop pipeline soon",
                  _pid.ShortDebugString().c_str(),
                  rawDoc->toString().c_str());
        return false;
    }

    // process
    auto processor = _pipeline->processor.get();
    build_service::document::RawDocumentVecPtr rawDocVec(new build_service::document::RawDocumentVec);
    rawDocVec->push_back(rawDoc);
    auto processedDocs = processor->process(rawDocVec);
    if (!processedDocs || processedDocs->size() != 1) {
        AUTIL_LOG(ERROR,
                  "%s: process fatal error, skip rawDoc: %s",
                  _pid.ShortDebugString().c_str(),
                  rawDoc->toString().c_str());
        return false;
    }

    const auto &processedDoc = (*processedDocs)[0];

    const auto &docBatch = processedDoc->getDocumentBatch();
    if (!docBatch || docBatch->GetBatchSize() != 1) {
        AUTIL_LOG(ERROR,
                  "%s: empty index document processed from %s",
                  _pid.ShortDebugString().c_str(),
                  rawDoc->toString().c_str());
        return false;
    }
    const auto &indexDoc = (*docBatch)[0];
    assert(indexDoc);
    indexDoc->SetLocator(*locator);

    // build
    auto builder = _pipeline->builder.get();
    if (buildWithRetry(builder, docBatch)) {
        return true;
    }
    if (builder->hasFatalError()) {
        AUTIL_LOG(WARN, "%s: fatal error happens, stop build and reload", _pid.ShortDebugString().c_str());
        _needReload = true;
    } else if (builder->needReconstruct()) {
        AUTIL_LOG(WARN, "%s: pipeline need reconstruct", _pid.ShortDebugString().c_str());
        // TODO: maybe recreate reader only, do not recreate processor/builder
        OP_LOG("stopBuildPipeline");
        autil::ScopedLock lock(_mutex);
        _pipeline->stop();
        _pipeline.reset();
    }
    return false;
}

bool DirectBuilder::buildWithRetry(build_service::builder::BuilderV2Impl *builder,
                                   const std::shared_ptr<indexlibv2::document::IDocumentBatch> &batch) {
    while (!_buildLoopStop) {
        AUTIL_LOG(TRACE3, "[%p] start build locator[%s]", this, batch->GetLastLocator().DebugString().c_str());
        auto ret = builder->build(batch);
        AUTIL_LOG(
            TRACE3, "[%p] end build locator[%s], ret[%d]", this, batch->GetLastLocator().DebugString().c_str(), ret);
        if (ret) {
            return true;
        }
        if (builder->hasFatalError() || builder->needReconstruct()) {
            return false;
        }
        // builder内部在NoMem情况下会sleep
        // sleep(1);
    }
    return false;
}

bool DirectBuilder::rewriteWithRetry(build_service::workflow::RawDocumentRewriter *rewriter,
                                     const std::shared_ptr<indexlibv2::document::RawDocument> &rawDoc) {
    assert(rawDoc);
    if (!rewriter) {
        return true;
    }
    int32_t retryCount = 0;
    while (!_buildLoopStop) {
        AUTIL_LOG(TRACE3, "[%p] start rewrite raw doc [%s]", this, rawDoc->toString().c_str());
        if (rewriter->rewrite(rawDoc.get())) {
            return true;
        }
        retryCount++;
        if (retryCount % 10 == 0) {
            AUTIL_LOG(WARN, "[%p] raw doc [%s] has retry [%d] times", this, rawDoc->toString().c_str(), retryCount);
        }
    }
    //这个方法只能在收到stop命令时才能返回false，否则需要一直重试
    //如果失败了若干次后返回false了，这个build loop会丢，导致丢doc
    return false;
}

void DirectBuilder::maybeInitBuildPipeline() {
    if (_pipeline) {
        return;
    }

    OP_LOG("createBuildPipeline");

    // reader
    auto reader = createReader(_resourceReader);
    if (!reader) {
        AUTIL_LOG(ERROR, "create reader for %s failed", _pid.ShortDebugString().c_str());
        return;
    }

    auto schema = _tablet->GetTabletSchema();
    if (!schema) {
        AUTIL_LOG(ERROR, "get tablet schema for %s failed", _pid.ShortDebugString().c_str());
        return;
    }
    // rewriter
    std::unique_ptr<build_service::workflow::RawDocumentRewriter> rewriter = createRawDocRewriter();

    // processor
    auto processor = createProcessor(_resourceReader, schema);
    if (!processor) {
        AUTIL_LOG(ERROR, "create processor for %s failed", _pid.ShortDebugString().c_str());
        return;
    }

    // builder
    auto builder = createBuilder(_resourceReader);
    if (!builder) {
        return;
    }

    auto pipeline = std::make_unique<Pipeline>();
    pipeline->reader = std::move(reader);
    pipeline->rewriter = std::move(rewriter);
    pipeline->processor = std::move(processor);
    pipeline->builder = std::move(builder);

    autil::ScopedLock lock(_mutex);
    _pipeline = std::move(pipeline);
}

std::unique_ptr<build_service::reader::RawDocumentReader>
DirectBuilder::createReader(const std::shared_ptr<build_service::config::ResourceReader> &resourceReader) const {
    OP_LOG("createReader");
    auto counterMap = _tablet->GetTabletInfos()->GetCounterMap();
    std::unique_ptr<build_service::reader::RawDocumentReader> reader(
        _readerCreatorAdapter->create(resourceReader,
                                      _rtBuilderResource->realtimeInfo.getKvMap(),
                                      _pid,
                                      _rtBuilderResource->metricProvider,
                                      counterMap,
                                      _tablet->GetTabletSchema()));
    if (!reader) {
        AUTIL_LOG(ERROR, "%s: create RawDocumentReader failed", _pid.ShortDebugString().c_str());
        return nullptr;
    }
    auto locator = _tablet->GetTabletInfos()->GetLatestLocator();
    if (!locator.IsValid() || locator.GetUserData().empty() ||
        (_srcSignature && _srcSignature.value() != locator.GetSrc())) {
        // TODO: maybe check srcId/srcSignature
        AUTIL_LOG(INFO,
                  "%s: invalid/old build locator[%s], do not seek",
                  _pid.ShortDebugString().c_str(),
                  locator.DebugString().c_str());
        return reader;
    }
    if (!locator.ShrinkToRange(_pid.range().from(), _pid.range().to())) {
        AUTIL_LOG(ERROR,
                  "%s: shrink range for locator[%s] failed",
                  _pid.ShortDebugString().c_str(),
                  locator.DebugString().c_str());
        return nullptr;
    }
    build_service::reader::Checkpoint ckpt(
        locator.GetOffset().first, locator.GetMultiProgress(), locator.GetUserData());
    AUTIL_LOG(INFO,
              "%s: seek to locator: %s, checkpoint: %s",
              _pid.ShortDebugString().c_str(),
              locator.DebugString().c_str(),
              ckpt.debugString().c_str());
    if (!reader->seek(ckpt)) {
        AUTIL_LOG(ERROR, "%s: seek to %s failed", _pid.ShortDebugString().c_str(), ckpt.debugString().c_str());
        return nullptr;
    }
    return reader;
}

std::unique_ptr<build_service::processor::Processor>
DirectBuilder::createProcessor(const std::shared_ptr<build_service::config::ResourceReader> &resourceReader,
                               const std::shared_ptr<indexlibv2::config::ITabletSchema> &schema) const {
    OP_LOG("createProcessor");
    std::string configPath = resourceReader->getOriginalConfigPath();
    std::string schemaJsonString;

    if (!schema->Serialize(/*isCompact*/ false, &schemaJsonString)) {
        AUTIL_LOG(ERROR, "serialize schema failed, pid [%s]", _pid.ShortDebugString().c_str());
        return nullptr;
    }
    auto tableName = _pid.clusternames(0);
    auto relativePath = build_service::config::ResourceReader::getSchemaConfRelativePath(tableName);
    auto schemaPath = fslib::util::FileUtil::joinFilePath(configPath, relativePath);
    if (!fslib::util::FileUtil::writeFile(schemaPath, schemaJsonString)) {
        AUTIL_LOG(
            ERROR, "write schema failed, pid [%s], path [%s]", _pid.ShortDebugString().c_str(), schemaPath.c_str());
        return nullptr;
    }
    auto newResourceReader = std::make_shared<build_service::config::ResourceReader>(configPath);

    auto processor = std::make_unique<build_service::processor::Processor>();
    auto configReader = std::make_shared<build_service::config::ProcessorConfigReader>(
        newResourceReader, _pid.buildid().datatable(), "");
    auto counterMap = _tablet->GetTabletInfos()->GetCounterMap();
    AUTIL_LOG(
        INFO, "create processor, pid [%s], schema id [%d]", _pid.ShortDebugString().c_str(), schema->GetSchemaId());
    build_service::KeyValueMap param;
    param["is_online_build"] = "true";
    if (!processor->start(configReader,
                          _pid,
                          _rtBuilderResource->metricProvider,
                          counterMap,
                          param,
                          /*forceSingleThreaded=*/true,
                          /*isTablet=*/true)) {
        AUTIL_LOG(
            ERROR, "start processor failed with config path : %s", resourceReader->getOriginalConfigPath().c_str());
        return nullptr;
    }
    return processor;
}

std::unique_ptr<build_service::builder::BuilderV2Impl>
DirectBuilder::createBuilder(const std::shared_ptr<build_service::config::ResourceReader> &resourceReader) const {
    OP_LOG("createBuilder");
    auto builder = std::make_unique<build_service::builder::BuilderV2Impl>(_tablet, _pid.buildid());

    build_service::config::BuilderClusterConfig clusterConfig;
    if (_pid.clusternames_size() != 1) {
        AUTIL_LOG(ERROR, "invalid pid %s", _pid.ShortDebugString().c_str());
        return nullptr;
    }
    if (!clusterConfig.init(_pid.clusternames(0), *resourceReader, "")) {
        AUTIL_LOG(ERROR, "failed to init BuilderClusterConfig for %s", _pid.ShortDebugString().c_str());
        return nullptr;
    }
    if (!builder->init(clusterConfig.builderConfig, _rtBuilderResource->metricProvider)) {
        AUTIL_LOG(ERROR, "init builder for %s failed", _pid.ShortDebugString().c_str());
        return nullptr;
    }
    return builder;
}

std::unique_ptr<build_service::workflow::RawDocumentRewriter> DirectBuilder::createRawDocRewriter() const {
    auto rewriter = std::make_unique<build_service::workflow::RawDocumentRewriter>();
    if (!rewriter->init(_tablet)) {
        AUTIL_LOG(WARN, "init raw doc rewriter for [%s] failed, will not do rewrite", _pid.ShortDebugString().c_str());
        return nullptr;
    }
    return rewriter;
}

bool DirectBuilder::processBulkloadDoc(const std::shared_ptr<indexlibv2::document::RawDocument> &rawDoc) {
    build_service::proto::BuildId buildId;
    auto buildIdRawStr = rawDoc->getField(BUILD_ID_KEY);
    std::string buildIdStr = autil::legacy::Base64DecodeFast(buildIdRawStr);
    if (buildIdStr.empty()) {
        AUTIL_LOG(ERROR,
                  "%s: skip bulkload doc, build id is decode from base64 return empty str",
                  _pid.ShortDebugString().c_str());
        return false;
    }
    if (!buildId.ParseFromString(buildIdStr)) {
        AUTIL_LOG(ERROR,
                  "%s: build id %s parse from string failed, raw doc %s",
                  _pid.ShortDebugString().c_str(),
                  buildIdStr.c_str(),
                  rawDoc->toString().c_str());
        return false;
    }
    if (buildId != _pid.buildid()) {
        AUTIL_LOG(WARN,
                  "%s: skip bulkload doc, build id %s is diff from pid build id %s, raw doc %s",
                  _pid.ShortDebugString().c_str(),
                  buildId.ShortDebugString().c_str(),
                  _pid.buildid().ShortDebugString().c_str(),
                  rawDoc->toString().c_str());
        return false;
    }
    std::string bulkloadId = rawDoc->getField(indexlibv2::framework::PARAM_BULKLOAD_ID);
    if (bulkloadId.empty()) {
        AUTIL_LOG(ERROR, "skip bulkload doc, bulkload id is empty, raw doc %s", rawDoc->toString().c_str());
        return false;
    }
    OP_LOG("bulkload");
    std::string externalFilesStr = rawDoc->getField(indexlibv2::framework::PARAM_EXTERNAL_FILES);
    std::string optionsStr = rawDoc->getField(indexlibv2::framework::PARAM_IMPORT_EXTERNAL_FILE_OPTIONS);
    std::string actionStr = rawDoc->getField(indexlibv2::framework::ACTION_KEY);
    std::string errMsg;
    auto [externalFiles, options, action] =
        BuildTask::prepareBulkloadParams(externalFilesStr, optionsStr, actionStr, &errMsg);
    if (!errMsg.empty()) {
        AUTIL_LOG(WARN, "%s", errMsg.c_str());
    }
    auto status = _tablet->ImportExternalFiles(
        bulkloadId, externalFiles, options, action, /*eventTimeInSecs=*/indexlib::INVALID_TIMESTAMP);
    if (status.IsOK()) {
        AUTIL_LOG(INFO, "%s: bulkload external files success", _pid.ShortDebugString().c_str());
    } else {
        if (status.IsInvalidArgs()) {
            AUTIL_LOG(
                ERROR, "%s: bulkload failed, error: %s", _pid.ShortDebugString().c_str(), status.ToString().c_str());
        } else {
            _needReload = true;
            AUTIL_LOG(ERROR,
                      "%s: bulkload failed, need reload, error: %s",
                      _pid.ShortDebugString().c_str(),
                      status.ToString().c_str());
        }
    }
    // do not need build
    return false;
}

bool DirectBuilder::processAlterTableDoc(const std::shared_ptr<indexlibv2::document::RawDocument> &rawDoc) {
    build_service::proto::BuildId buildId;
    auto buildIdStr = rawDoc->getField(BUILD_ID_KEY);
    if (!buildId.ParseFromString(buildIdStr)) {
        AUTIL_LOG(ERROR,
                  "%s: build id %s parse from string failed, raw doc %s",
                  _pid.ShortDebugString().c_str(),
                  buildIdStr.c_str(),
                  rawDoc->toString().c_str());
        return false;
    }

    if (buildId != _pid.buildid()) {
        AUTIL_LOG(WARN,
                  "%s: skip alter tablet doc build id %s is diff from pid build id %s, raw doc %s",
                  _pid.ShortDebugString().c_str(),
                  buildId.ShortDebugString().c_str(),
                  _pid.buildid().ShortDebugString().c_str(),
                  rawDoc->toString().c_str());
        return false;
    }

    auto configPath = rawDoc->getField(CONFIG_PATH_KEY);
    uint32_t schemaVersion = 0;
    auto schemaVersionStr = rawDoc->getField(SCHEMA_VERSION_KEY);
    if (!autil::StringUtil::fromString(schemaVersionStr, schemaVersion)) {
        AUTIL_LOG(ERROR, "%s: schema version %s is invalid", _pid.ShortDebugString().c_str(), schemaVersionStr.c_str());
        return false;
    }
    if (!alterTable(schemaVersion, configPath)) {
        AUTIL_LOG(ERROR,
                  "%s: alter table with config %s failed, ignore it",
                  _pid.ShortDebugString().c_str(),
                  configPath.c_str());
    }
    // do not need build
    return false;
}

bool DirectBuilder::alterTable(uint32_t version, const std::string &configPath) {
    OP_LOG("alterTable");

    // aliws init need local config path
    auto localConfigPath = TablePathDefine::constructTempConfigPath(
        _pid.clusternames(0), _pid.range().from(), _pid.range().to(), configPath);
    if (localConfigPath.empty()) {
        AUTIL_LOG(ERROR, "config path invalid: %s", configPath.c_str());
        return false;
    }
    auto errorCode = build_service::common::ConfigDownloader::downloadConfig(configPath, localConfigPath);

    if (errorCode == build_service::common::ConfigDownloader::DEC_NORMAL_ERROR ||
        errorCode == build_service::common::ConfigDownloader::DEC_DEST_ERROR) {
        AUTIL_LOG(ERROR, "download config from [%s] to [%s] failed", configPath.c_str(), localConfigPath.c_str());
        return false;
    }

    auto resourceReader = std::make_shared<build_service::config::ResourceReader>(localConfigPath);
    const auto &clusterName = _pid.clusternames(0);
    auto schema = resourceReader->getTabletSchema(clusterName);
    if (!schema) {
        AUTIL_LOG(ERROR, "%s: load schema from %s failed", _pid.ShortDebugString().c_str(), localConfigPath.c_str());
        return false;
    }

    AUTIL_LOG(INFO,
              "%s: try force set schema version from %u to %u",
              _pid.ShortDebugString().c_str(),
              schema->GetSchemaId(),
              version);
    if (version > schema->GetSchemaId()) {
        AUTIL_LOG(INFO,
                  "%s: force set schema version from %u to %u",
                  _pid.ShortDebugString().c_str(),
                  schema->GetSchemaId(),
                  version);
        schema->SetSchemaId(version);
    }

    // reload resource reader
    auto processor = createProcessor(resourceReader, schema);
    if (!processor) {
        AUTIL_LOG(ERROR,
                  "%s: alterTable with config %s failed, can not start processor",
                  _pid.ShortDebugString().c_str(),
                  localConfigPath.c_str());
        return false;
    }

    auto s = _tablet->AlterTable(schema);
    if (s.IsOK()) {
        AUTIL_LOG(INFO,
                  "%s: alter table with version %u, config %s success",
                  _pid.ShortDebugString().c_str(),
                  version,
                  localConfigPath.c_str());
        autil::ScopedLock lock(_mutex);
        _pipeline->processor = std::move(processor);
        return true;
    } else {
        AUTIL_LOG(ERROR,
                  "%s: alter table with invalid config[%s], error: %s",
                  _pid.ShortDebugString().c_str(),
                  localConfigPath.c_str(),
                  s.ToString().c_str());
        if (!s.IsInvalidArgs()) {
            _needReload = true;
        }
        return false;
    }
}

void DirectBuilder::setLastLocator(const std::shared_ptr<indexlibv2::framework::Locator> &locator) {
    autil::ScopedLock lock(_mutex);
    _lastLocator = locator;
}

#undef OP_LOG

} // namespace suez
