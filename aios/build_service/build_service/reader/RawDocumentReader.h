#ifndef ISEARCH_BS_RAWDOCUMENTREADER_H
#define ISEARCH_BS_RAWDOCUMENTREADER_H

#include <indexlib/util/counter/counter_map.h>
#include <indexlib/util/counter/counter.h>
#include <indexlib/document/document_factory_wrapper.h>
#include <indexlib/document/raw_document_parser.h>
#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/document/RawDocument.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/proto/ErrorCollector.h"
#include "build_service/document/RawDocumentHashMapManager.h"
#include "build_service/common/End2EndLatencyReporter.h"

DECLARE_REFERENCE_CLASS(util, AccumulativeCounter);

IE_NAMESPACE_BEGIN(misc);
class MetricProvider;
typedef std::shared_ptr<MetricProvider> MetricProviderPtr;
class Metric;
typedef std::shared_ptr<Metric> MetricPtr;
IE_NAMESPACE_END(misc);

namespace build_service {
namespace reader {

struct ReaderInitParam {
    ReaderInitParam()
        : metricProvider(NULL) {}
    KeyValueMap kvMap;
    config::ResourceReaderPtr resourceReader;
    proto::Range range;
    IE_NAMESPACE(misc)::MetricProviderPtr metricProvider;
    IE_NAMESPACE(util)::CounterMapPtr counterMap;
    IE_NAMESPACE(config)::IndexPartitionSchemaPtr schema;
    proto::PartitionId partitionId;
};

class RawDocumentReader : public proto::ErrorCollector
{
public:
    enum ErrorCode {
        ERROR_NONE,
        ERROR_EOF,
        ERROR_PARSE,
        ERROR_WAIT,
        ERROR_EXCEPTION
    };

    enum ExceedTsAction {
        ETA_SUSPEND,
        ETA_STOP,
        ETA_UNKOWN
    };

    using Message = IE_NAMESPACE(document)::RawDocumentParser::Message;

public:
    RawDocumentReader();
    virtual ~RawDocumentReader();
private:
    RawDocumentReader(const RawDocumentReader &);
    RawDocumentReader& operator=(const RawDocumentReader &);
public:
    void initBeeperTagsFromPartitionId(const proto::PartitionId& pid);
    bool initialize(const ReaderInitParam &params);
    ErrorCode read(document::RawDocumentPtr &rawDoc, int64_t &offset);
protected:
    virtual ErrorCode read(document::RawDocument &rawDoc, int64_t &offset);
public:
    virtual bool init(const ReaderInitParam &params);
    virtual bool seek(int64_t offset) = 0;
    virtual bool isEof() const = 0;
    virtual void suspendReadAtTimestamp(int64_t targetTimestamp, ExceedTsAction action) {}
    virtual bool isExceedSuspendTimestamp() const { return false; }

    virtual bool getMaxTimestampAfterStartTimestamp(int64_t &timestamp);
    virtual bool isStreamReader() { return false; }

protected:
    virtual ErrorCode readDocStr(std::string &docStr, int64_t &offset, int64_t &timestamp) = 0;
    virtual ErrorCode readDocStr(std::vector<Message> &msgs, int64_t &offset, size_t maxMessageCount) {
        msgs.clear();
        for (size_t i = 0; i < maxMessageCount; ++i) {
            msgs.emplace_back();
            auto &msg = msgs.back();
            auto ret = readDocStr(msg.data, offset, msg.timestamp);
            if (ret != ERROR_NONE) {
                return ret;
            }
        }
        return ERROR_NONE;
    }
    virtual IE_NAMESPACE(document)::RawDocumentParser *createRawDocumentParser(
        const ReaderInitParam &params);
    virtual int64_t getFreshness();
    virtual bool initDocumentFactoryWrapper(const ReaderInitParam &params);
    bool initBatchBuild(const ReaderInitParam &params);
    bool initMetrics(IE_NAMESPACE(misc)::MetricProviderPtr metricProvider);

protected:
    IE_NAMESPACE(document)::RawDocumentParser *_parser;
    document::RawDocumentHashMapManagerPtr _hashMapManager;
    std::string _timestampFieldName;
    std::string _sourceFieldName;

    IE_NAMESPACE(document)::DocumentFactoryWrapperPtr _documentFactoryWrapper;

private:
    IE_NAMESPACE(util)::AccumulativeCounterPtr _parseFailCounter;
    common::End2EndLatencyReporter _e2eLatencyReporter;

    IE_NAMESPACE(misc)::MetricPtr _rawDocSizeMetric;
    IE_NAMESPACE(misc)::MetricPtr _rawDocSizePerSecMetric;
    IE_NAMESPACE(misc)::MetricPtr _readLatencyMetric;
    IE_NAMESPACE(misc)::MetricPtr _parseLatencyMetric;
    IE_NAMESPACE(misc)::MetricPtr _parseErrorQpsMetric;

    std::string _traceField;
    std::string _extendFieldName;
    std::string _extendFieldValue;
    size_t _batchBuildSize;
    std::vector<Message> _msgBuffer;

private:
    BS_LOG_DECLARE();
};

}
}

#endif //ISEARCH_BS_RAWDOCUMENTREADER_H
