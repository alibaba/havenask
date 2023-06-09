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
#ifndef ISEARCH_BS_RAWDOCUMENTREADER_H
#define ISEARCH_BS_RAWDOCUMENTREADER_H

#include "build_service/common/End2EndLatencyReporter.h"
#include "build_service/common/ExceedTsAction.h"
#include "build_service/common_define.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/document/RawDocument.h"
#include "build_service/document/RawDocumentHashMapManager.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/proto/ErrorCollector.h"
#include "build_service/util/Log.h"
#include "indexlib/document/document_factory_wrapper.h"
#include "indexlib/document/query/query_evaluator_creator.h"
#include "indexlib/document/raw_document_parser.h"
#include "indexlib/util/counter/Counter.h"
#include "indexlib/util/counter/CounterMap.h"

DECLARE_REFERENCE_CLASS(util, AccumulativeCounter);

namespace indexlib { namespace seekutil {
class MetricProvider;
typedef std::shared_ptr<MetricProvider> MetricProviderPtr;
class Metric;
typedef std::shared_ptr<Metric> MetricPtr;
}} // namespace indexlib::seekutil

namespace build_service { namespace common {
class PeriodDocCounterBase;
}} // namespace build_service::common

namespace indexlibv2::config {
class TabletSchema;
}
namespace indexlibv2::document {
class IDocumentFactory;
}

namespace build_service { namespace reader {
class HologresInterface;

struct ReaderInitParam {
    ReaderInitParam() : metricProvider(NULL), hologresInterface(NULL) {}
    KeyValueMap kvMap;
    config::ResourceReaderPtr resourceReader;
    proto::Range range;
    indexlib::util::MetricProviderPtr metricProvider;
    indexlib::util::CounterMapPtr counterMap;
    indexlib::config::IndexPartitionSchemaPtr schema;
    std::shared_ptr<indexlibv2::config::TabletSchema> schemaV2;
    proto::PartitionId partitionId;
    std::shared_ptr<HologresInterface> hologresInterface;
};

struct DocInfo {
    uint16_t hashId;
    int64_t docTimestamp;
    DocInfo(uint16_t hashId = 0, int64_t docTimestamp = 0) : hashId(hashId), docTimestamp(docTimestamp) {}
};

struct Checkpoint {
    Checkpoint() {}
    Checkpoint(int64_t offset, const std::string& userData) : offset(offset), userData(userData) {}
    Checkpoint(int64_t offset, const std::vector<indexlibv2::base::Progress>& progress, const std::string& userData)
        : offset(offset)
        , progress(progress)
        , userData(userData)
    {
    }
    // offset should be a monotonic growing number and is used to compare which checkpoint is newer.
    int64_t offset = 0;
    std::vector<indexlibv2::base::Progress> progress;
    // userData is optional and can be filled if the actual checkpoint needs to be a custom type other than integer.
    std::string userData;

    bool operator==(const Checkpoint& other) const
    {
        return offset == other.offset && progress == other.progress && userData == other.userData;
    }
    bool operator!=(const Checkpoint& other) const { return !(*this == other); }

    std::string debugString() const
    {
        std::stringstream ss;
        ss << offset << ':' << userData << ':';
        for (const auto& oneProgress : progress) {
            ss << '[' << oneProgress.from << ':' << oneProgress.to << ':' << oneProgress.offset << ']';
        }
        return ss.str();
    }
};

class RawDocumentReader : public proto::ErrorCollector
{
public:
    enum ErrorCode { ERROR_NONE, ERROR_EOF, ERROR_PARSE, ERROR_WAIT, ERROR_EXCEPTION, ERROR_SEALED };

    using Message = indexlib::document::RawDocumentParser::Message;

public:
    RawDocumentReader();
    virtual ~RawDocumentReader();

private:
    RawDocumentReader(const RawDocumentReader&);
    RawDocumentReader& operator=(const RawDocumentReader&);

public:
    void initBeeperTagsFromPartitionId(const proto::PartitionId& pid);
    bool initialize(const ReaderInitParam& params);
    ErrorCode read(document::RawDocumentPtr& rawDoc, Checkpoint* checkpoint);
    common::End2EndLatencyReporter& GetEnd2EndLatencyReporter() { return _e2eLatencyReporter; }

protected:
    virtual ErrorCode read(document::RawDocument& rawDoc, Checkpoint* checkpoint);
    virtual void doFillDocTags(document::RawDocument& rawDoc) {}

public:
    // init() failures will be cheated as fatal error and might not be retried by bs framework.
    // If e.g. network, machine failures etc might happen during init(), retry should be handled internally.
    virtual bool init(const ReaderInitParam& params);
    virtual bool seek(const Checkpoint& checkpoint) = 0;
    virtual bool isEof() const = 0;
    virtual void suspendReadAtTimestamp(int64_t targetTimestamp, common::ExceedTsAction action) {}
    virtual bool isExceedSuspendTimestamp() const { return false; }
    virtual ErrorCode getNextRawDoc(document::RawDocument& rawDoc, Checkpoint* checkpoint, DocInfo& docInfo);
    virtual bool getMaxTimestampAfterStartTimestamp(int64_t& timestamp);
    virtual bool isStreamReader() { return false; }

protected:
    virtual ErrorCode readDocStr(std::string& docStr, Checkpoint* checkpoint, DocInfo& docInfo) = 0;
    virtual ErrorCode readDocStr(std::vector<Message>& msgs, Checkpoint* checkpoint, size_t maxMessageCount)
    {
        msgs.clear();
        for (size_t i = 0; i < maxMessageCount; ++i) {
            msgs.emplace_back();
            auto& msg = msgs.back();
            DocInfo docInfo;
            auto ret = readDocStr(msg.data, checkpoint, docInfo);
            msg.timestamp = docInfo.docTimestamp;
            msg.hashId = docInfo.hashId;
            if (ret != ERROR_NONE) {
                return ret;
            }
        }
        return ERROR_NONE;
    }
    virtual indexlib::document::RawDocumentParser* createRawDocumentParser(const ReaderInitParam& params);
    virtual bool initDocumentFactoryWrapper(const ReaderInitParam& params);
    bool initDocumentFactoryV2(const ReaderInitParam& params);

    // TODO: remove this when support getting user timestamp from swift msg
    virtual void reportTimestampFieldValue(int64_t value) {}

    bool initBatchBuild(const ReaderInitParam& params);
    bool initMetrics(indexlib::util::MetricProviderPtr metricProvider);
    bool initRawDocQueryEvaluator(const ReaderInitParam& params);
    void fillDocTags(document::RawDocument& rawDoc);

protected:
    indexlib::document::RawDocumentParser* _parser;
    document::RawDocumentHashMapManagerPtr _hashMapManager;
    std::string _timestampFieldName;
    std::string _sourceFieldName;
    bool _needDocTag;

    indexlib::document::DocumentFactoryWrapperPtr _documentFactoryWrapper;
    std::unique_ptr<indexlibv2::document::IDocumentFactory> _documentFactoryV2;
    std::shared_ptr<indexlibv2::config::TabletSchema> _tabletSchema;
    indexlib::document::QueryEvaluatorCreatorPtr _evaluatorCreator;
    indexlib::document::QueryEvaluatorPtr _queryEvaluator;
    indexlib::document::QueryEvaluator::EvaluateParam _evaluateParam;

protected:
    indexlib::util::AccumulativeCounterPtr _parseFailCounter;
    common::End2EndLatencyReporter _e2eLatencyReporter;

    indexlib::util::MetricPtr _rawDocSizeMetric;
    indexlib::util::MetricPtr _rawDocSizePerSecMetric;
    indexlib::util::MetricPtr _readLatencyMetric;
    indexlib::util::MetricPtr _getNextDocLatencyMetric;
    indexlib::util::MetricPtr _parseLatencyMetric;
    indexlib::util::MetricPtr _parseErrorQpsMetric;
    indexlib::util::MetricPtr _readDocQpsMetric;
    indexlib::util::MetricPtr _filterDocQpsMetric;

private:
    std::string _traceField;
    std::string _extendFieldName;
    std::string _extendFieldValue;
    std::string _sourceFieldDefaultValue;
    size_t _batchBuildSize;
    std::vector<Message> _msgBuffer;
    common::PeriodDocCounterBase* _docCounter;
    int64_t _checkpointStep;
    bool _enableIngestionTimestamp;
    static constexpr int64_t DEFAULT_TRIGGER_CHECKPOINT_STEP = 1000;

private:
    BS_LOG_DECLARE();
};
BS_TYPEDEF_PTR(RawDocumentReader);
}} // namespace build_service::reader

#endif // ISEARCH_BS_RAWDOCUMENTREADER_H
