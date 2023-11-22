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
#include "suez/sdk/TableWriter.h"

#include <future_lite/uthread/Latch.h>

#include "RawDocument2SwiftFieldFilter.h"
#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/result/Errors.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/proto/ParserConfig.h"
#include "build_service/reader/DocumentSeparators.h"
#include "build_service/reader/ParserCreator.h"
#include "build_service/reader/RawDocumentBuilder.h"
#include "build_service/util/LocatorUtil.h"
#include "build_service/util/SwiftClientCreator.h"
#include "indexlib/document/document_factory_wrapper.h"
#include "indexlib/document/raw_document/raw_document_define.h"
#include "indexlib/document/raw_document_parser.h"
#include "indexlib/framework/ITablet.h"
#include "indexlib/framework/Locator.h"
#include "indexlib/framework/TabletInfos.h"
#include "indexlib/indexlib.h"
#include "kmonitor/client/MetricType.h"
#include "kmonitor/client/MetricsReporter.h"
#include "suez/table/wal/CommonDefine.h"
#include "suez/table/wal/WALConfig.h"
#include "suez/table/wal/WALStrategy.h"

namespace suez {
AUTIL_DECLARE_AND_SETUP_LOGGER(suez, TableWriter);

using autil::result::RuntimeError;

struct WriteMetricsCollector {
    WriteMetricsCollector() { startTime = autil::TimeUtility::currentTimeInMicroSeconds(); }

    void writeEnd() {
        auto endTime = autil::TimeUtility::currentTimeInMicroSeconds();
        latency = endTime - startTime;
    }
    int64_t startTime = 0;

    size_t inMessageCount = 0;
    int64_t latency = 0;
    int64_t parseLatency = 0;
    int64_t logLatency = 0;
    int64_t buildLatency = 0;
    int64_t buildGap = 0;
    bool writeSuccess = false;
};

class WriteMetrics : public kmonitor::MetricsGroup {
public:
    bool init(kmonitor::MetricsGroupManager *manager) override {
        REGISTER_GAUGE_MUTABLE_METRIC(_inMessageCount, "inMessageCount");
        REGISTER_LATENCY_MUTABLE_METRIC(_latency, "latency");
        REGISTER_LATENCY_MUTABLE_METRIC(_parseLatency, "parseLatency");
        REGISTER_LATENCY_MUTABLE_METRIC(_logLatency, "logLatency");
        REGISTER_LATENCY_MUTABLE_METRIC(_buildLatency, "buildLatency");
        REGISTER_LATENCY_MUTABLE_METRIC(_buildGap, "buildGap");
        REGISTER_QPS_MUTABLE_METRIC(_buildGap0, "buildGap0");
        REGISTER_QPS_MUTABLE_METRIC(_buildGap10, "buildGapIn10ms");
        REGISTER_QPS_MUTABLE_METRIC(_buildGap100, "buildGapIn100ms");
        REGISTER_QPS_MUTABLE_METRIC(_buildGap1000, "buildGapIn1000ms");
        REGISTER_QPS_MUTABLE_METRIC(_buildGapLarge1000, "buildGapLarge1000ms");

        REGISTER_QPS_MUTABLE_METRIC(_inQps, "inQps");
        REGISTER_QPS_MUTABLE_METRIC(_errorQps, "errorQps");
        REGISTER_QPS_MUTABLE_METRIC(_emptyQps, "emptyQps");
        return true;
    }

    void report(const kmonitor::MetricsTags *tags, WriteMetricsCollector *collector) {
        REPORT_MUTABLE_METRIC(_inMessageCount, collector->inMessageCount);
        REPORT_MUTABLE_METRIC(_latency, collector->latency);
        if (collector->parseLatency > 0) {
            REPORT_MUTABLE_METRIC(_parseLatency, collector->parseLatency);
        }
        if (collector->logLatency > 0) {
            REPORT_MUTABLE_METRIC(_logLatency, collector->logLatency);
        }
        if (collector->buildLatency > 0) {
            REPORT_MUTABLE_METRIC(_buildLatency, collector->buildLatency);
        }
        if (collector->buildGap > 0) {
            if (collector->buildGap <= 10000) {
                REPORT_MUTABLE_QPS(_buildGap10);
            } else if (collector->buildGap <= 100000) {
                REPORT_MUTABLE_QPS(_buildGap100);
            } else if (collector->buildGap <= 1000000) {
                REPORT_MUTABLE_QPS(_buildGap1000);
            } else {
                REPORT_MUTABLE_QPS(_buildGapLarge1000);
            }
            REPORT_MUTABLE_METRIC(_buildGap, collector->buildGap);

        } else {
            REPORT_MUTABLE_QPS(_buildGap0);
        }
        REPORT_MUTABLE_QPS(_inQps);
        if (!collector->writeSuccess) {
            REPORT_MUTABLE_QPS(_errorQps);
        } else if (collector->inMessageCount == 0) {
            REPORT_MUTABLE_QPS(_emptyQps);
        }
    }

private:
    kmonitor::MutableMetric *_inMessageCount = nullptr;
    kmonitor::MutableMetric *_latency = nullptr;
    kmonitor::MutableMetric *_parseLatency = nullptr;
    kmonitor::MutableMetric *_logLatency = nullptr;
    kmonitor::MutableMetric *_buildLatency = nullptr;
    kmonitor::MutableMetric *_buildGap = nullptr;
    kmonitor::MutableMetric *_buildGap0 = nullptr;
    kmonitor::MutableMetric *_buildGap10 = nullptr;
    kmonitor::MutableMetric *_buildGap100 = nullptr;
    kmonitor::MutableMetric *_buildGap1000 = nullptr;
    kmonitor::MutableMetric *_buildGapLarge1000 = nullptr;
    kmonitor::MutableMetric *_inQps = nullptr;
    kmonitor::MutableMetric *_errorQps = nullptr;
    kmonitor::MutableMetric *_emptyQps = nullptr;
};

TableWriter::TableWriter() {}

TableWriter::~TableWriter() { stop(); }

bool TableWriter::init(const build_service::proto::PartitionId &pid,
                       const std::string &configPath, // for document parser
                       const kmonitor::MetricsReporterPtr &reporter,
                       const WALConfig &walConfig,
                       const std::shared_ptr<build_service::util::SwiftClientCreator> &swiftClientCreator) {
    _pid = std::make_unique<build_service::proto::PartitionId>(pid);
    _reporter = reporter;
    _walConfig = std::make_unique<WALConfig>(walConfig);
    _walConfig->desc = _pid->range().ShortDebugString();
    _swiftClientCreator = swiftClientCreator;

    auto resourceReader = std::make_shared<build_service::config::ResourceReader>(configPath);
    _documentFactoryWrapper = indexlib::document::DocumentFactoryWrapper::CreateDocumentFactoryWrapper(
        nullptr, indexlib::config::CUSTOMIZED_DOCUMENT_CONFIG_RAWDOCUMENT, resourceReader->getPluginPath());
    if (!_documentFactoryWrapper) {
        AUTIL_LOG(ERROR, "%s: init DocumentFactoryWrapper failed", _pid->ShortDebugString().c_str());
        return false;
    }
    // 容忍init wal失败，避免swift挂掉雪崩，当前实现是在rpc请求里重试，可能需要一个后台线程来自动重连
    autil::ScopedLock lock(_mutex);
    maybeInitWALLocked();
    return true;
}

void TableWriter::setEnableWrite(bool flag) {
    bool enableWrite = _enableWrite.exchange(flag);
    if (enableWrite && !flag) {
        AUTIL_LOG(INFO, "%s: writer changed from enabled to disabled", _pid->ShortDebugString().c_str());
        autil::ScopedLock lock(_mutex);
        if (_wal) {
            AUTIL_LOG(INFO, "%s: begin flushing log", _pid->ShortDebugString().c_str());
            _wal->flush();
            AUTIL_LOG(INFO, "%s: finish flushing log", _pid->ShortDebugString().c_str());
        }
    }
    AUTIL_LOG(INFO, "%s: writer is %s", _pid->ShortDebugString().c_str(), flag ? "enabled" : "disabled");
}

void TableWriter::write(const std::string &format,
                        const WalDocVector &docs,
                        const std::function<void(autil::Result<WriteResult>)> &done,
                        future_lite::Executor *executor) {
    if (!_enableWrite) {
        done(RuntimeError::make("%s is disabled, can not write", _pid->ShortDebugString().c_str()));
        return;
    }

    auto deleter = [this](WriteMetricsCollector *collector) {
        if (collector != nullptr) {
            collector->writeEnd();
            if (_reporter) {
                kmonitor::MetricsTags tags{{{"table_name", _pid->clusternames(0)}, {"role_type", _roleType}}};
                auto reporter = _reporter->getSubReporter("", tags);
                reporter->report<WriteMetrics>(nullptr, collector);
            }
            delete collector;
        }
    };
    std::shared_ptr<WriteMetricsCollector> collector(new WriteMetricsCollector(), std::move(deleter));

    collector->inMessageCount = docs.size();
    autil::ScopedTime2 parseT;
    auto result = parseWalDocs(format, docs);
    collector->parseLatency = parseT.done_us();
    if (!result.is_ok()) {
        done(std::move(result).steal_error());
        return;
    }

    auto walDocs = std::move(result).steal_value();
    if (walDocs.empty()) {
        // empty qps
        collector->writeSuccess = true;
        done({});
        return;
    }

    auto logStartTime = autil::TimeUtility::currentTimeInMicroSeconds();
    auto logDone =
        [this, logStartTime, collector_ = std::move(collector), done](autil::Result<std::vector<int64_t>> ret) {
            collector_->logLatency = autil::TimeUtility::currentTimeInMicroSeconds() - logStartTime;
            if (!ret.is_ok()) {
                done(std::move(ret).steal_error());
                return;
            }
            collector_->writeSuccess = true;
            collector_->writeEnd();
            auto values = std::move(ret).steal_value();
            updateLatestLogOffset(values.back());
            // TODO: maybe wait build to hold read on write
            WriteResult result;
            fillWriteResult(result, collector_.get());
            collector_->buildGap = result.watermark.buildGap;
            done(result);
        };

    if (executor != nullptr) {
        autil::Result<std::vector<int64_t>> logResult;
        future_lite::uthread::Latch latch(1);
        auto latchDone = [&latch, &logResult](autil::Result<std::vector<int64_t>> ret) {
            logResult = std::move(ret);
            latch.downCount();
        };
        DoWrite(walDocs, latchDone);
        latch.await(executor);
        logDone(std::move(logResult));
    } else {
        DoWrite(walDocs, logDone);
    }
}

void TableWriter::DoWrite(const WalDocVector &docs,
                          const std::function<void(autil::Result<std::vector<int64_t>>)> &done) {
    autil::ScopedLock lock(_mutex);
    maybeInitWALLocked();
    if (!_wal) {
        done(RuntimeError::make("%s wal invalid", _pid->ShortDebugString().c_str()));
        return;
    }
    _wal->log(docs, done);
}

void TableWriter::updateSchema(uint32_t version,
                               const std::string &configPath,
                               std::function<void(autil::Result<int64_t>)> done) {
    AUTIL_LOG(INFO, "update schema, version: [%u], config path: [%s]", version, configPath.c_str());
    if (!_enableWrite) {
        done(RuntimeError::make("%s is disabled, can not write", _pid->ShortDebugString().c_str()));
        return;
    }

    std::string buildIdStr;
    if (!_pid->buildid().SerializeToString(&buildIdStr)) {
        done(RuntimeError::make("%s buildid serialize failed, can not write", _pid->ShortDebugString().c_str()));
        return;
    }
    build_service::reader::RawDocumentBuilder builder(build_service::reader::RAW_DOCUMENT_HA3_SEP_PREFIX,
                                                      build_service::reader::RAW_DOCUMENT_HA3_SEP_SUFFIX,
                                                      build_service::reader::RAW_DOCUMENT_HA3_FIELD_SEP,
                                                      build_service::reader::RAW_DOCUMENT_HA3_KV_SEP);
    builder.addField(autil::StringView(indexlib::document::CMD_TAG), indexlib::document::CMD_ALTER);
    builder.addField(autil::StringView(CONFIG_PATH_KEY), configPath);
    builder.addField(autil::StringView(SCHEMA_VERSION_KEY), std::to_string(version));
    builder.addField(autil::StringView(BUILD_ID_KEY), buildIdStr);

    auto docStr = builder.finalize().to_string();
    auto walDocs = parseWalDocs(build_service::reader::RAW_DOCUMENT_HA3_DOCUMENT_FORMAT,
                                {{_pid->range().from(), std::move(docStr)}});
    if (!walDocs.is_ok()) {
        done(std::move(walDocs).steal_error());
        return;
    }

    autil::ScopedLock lock(_mutex);
    maybeInitWALLocked();
    if (!_wal) {
        done(RuntimeError::make("%s wal invalid", _pid->ShortDebugString().c_str()));
        return;
    }
    auto logDone = [this, done_ = std::move(done)](autil::Result<std::vector<int64_t>> ret) {
        if (!ret.is_ok()) {
            done_(std::move(ret).steal_error());
            return;
        }
        auto values = std::move(ret).steal_value();
        updateLatestLogOffset(values.back());
        done_(values.back());
    };
    _wal->log(std::move(walDocs).steal_value(), std::move(logDone));
    AUTIL_LOG(INFO, "update schema success, config path [%s]", configPath.c_str());
}

void TableWriter::stop() {
    autil::ScopedLock lock(_mutex);
    bool enableWrite = _enableWrite.exchange(false);
    if (enableWrite && _wal) {
        _wal->flush();
    }
    if (_wal) {
        _wal->stop();
        _wal.reset();
    }
    updateIndex(nullptr);
}

void TableWriter::updateIndex(const std::shared_ptr<indexlibv2::framework::ITablet> &index) {
    autil::ScopedWriteLock lock(_indexLock);
    _index = index;
}

std::shared_ptr<indexlibv2::framework::ITablet> TableWriter::getIndex() const {
    autil::ScopedReadLock lock(_indexLock);
    return _index;
}

void TableWriter::maybeInitWALLocked() {
    if (_wal) {
        return;
    }
    auto wal = WALStrategy::create(*_walConfig, _swiftClientCreator);
    if (!wal) {
        AUTIL_LOG(ERROR,
                  "%s: create wal with %s failed",
                  _pid->ShortDebugString().c_str(),
                  FastToJsonString(*_walConfig, true).c_str());
        return;
    }
    _wal = std::move(wal);
}

autil::Result<WalDocVector> TableWriter::parseWalDocs(const std::string &format, const WalDocVector &docs) {
    // parse
    auto parser = getParser(format);
    if (!parser) {
        return RuntimeError::make("%s unknown format %s", _pid->ShortDebugString().c_str(), format.c_str());
    }

    std::vector<indexlib::document::RawDocumentPtr> rawDocs;
    rawDocs.reserve(docs.size());
    int64_t currentTime = autil::TimeUtility::currentTimeInMicroSeconds();
    auto currentTimeStr = autil::StringUtil::toString(currentTime);
    for (const auto &doc : docs) {
        indexlib::document::RawDocumentPtr rawDoc(_documentFactoryWrapper->CreateRawDocument());
        if (!parser->parse(doc.second, *rawDoc)) {
            return RuntimeError::make("%s parse %s failed", _pid->ShortDebugString().c_str(), doc.second.c_str());
        }
        rawDoc->setDocTimestamp(currentTime);
        rawDoc->setField(HA_RESERVED_TIMESTAMP, currentTimeStr);
        rawDocs.emplace_back(std::move(rawDoc));
    }

    // format
    WalDocVector walDocs;
    walDocs.reserve(rawDocs.size());
    auto formatDocs = convertRawDocument2SwiftFieldFilter(rawDocs);
    for (auto i = 0; i < docs.size(); ++i) {
        walDocs.emplace_back(docs[i].first, std::move(formatDocs[i]));
    }
    return std::move(walDocs);
}

std::unique_ptr<indexlib::document::RawDocumentParser> TableWriter::getParser(const std::string &format) {
    // TODO: maybe cache
    build_service::proto::ParserConfig parserConfig;
    parserConfig.type = format;
    build_service::reader::ParserCreator parserCreator;
    std::unique_ptr<indexlib::document::RawDocumentParser> parser;
    parser.reset(parserCreator.createSingleParser(parserConfig));
    if (!parser) {
        AUTIL_LOG(ERROR,
                  "%s: create parser with format [%s] failed, error: %s",
                  _pid->ShortDebugString().c_str(),
                  format.c_str(),
                  parserCreator.getLastError().c_str());
        return nullptr;
    }
    return parser;
}

void TableWriter::updateLatestLogOffset(int64_t latestLogOffset) {
    _latestLogOffset.store(latestLogOffset, std::memory_order_relaxed);
}

int64_t TableWriter::getLatestLogOffset() const { return _latestLogOffset.load(std::memory_order_relaxed); }

void TableWriter::fillWriteResult(WriteResult &result, const WriteMetricsCollector *collector) const {
    result.stat.inMessageCount = collector->inMessageCount;
    result.stat.parseLatency = collector->parseLatency;
    result.stat.logLatency = collector->logLatency;
    result.stat.buildLatency = collector->buildLatency;

    // fill watermark
    result.watermark.maxCp = getLatestLogOffset();
    auto index = getIndex();
    if (!index) {
        result.state = WriterState::LOG;
        return;
    }
    result.state = WriterState::ASYNC; // TODO: support sync
    auto locator = index->GetTabletInfos()->GetLatestLocator();
    if (locator.IsValid()) {
        result.watermark.buildLocatorOffset = build_service::util::LocatorUtil::getSwiftWatermark(locator);
    }
    if (result.watermark.maxCp > 0) {
        result.watermark.buildGap = result.watermark.maxCp - std::max(0L, result.watermark.buildLocatorOffset);
    }
}

} // namespace suez
