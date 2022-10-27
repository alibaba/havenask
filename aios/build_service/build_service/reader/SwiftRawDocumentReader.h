#ifndef ISEARCH_BS_SWIFTRAWDOCUMENTREADER_H
#define ISEARCH_BS_SWIFTRAWDOCUMENTREADER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/reader/RawDocumentReader.h"
#include "build_service/reader/SwiftReaderWrapper.h"
#include "build_service/util/SwiftClientCreator.h"


IE_NAMESPACE_BEGIN(misc);
class Metric;
typedef std::shared_ptr<Metric> MetricPtr;
IE_NAMESPACE_END(misc);


namespace build_service {
namespace reader {

class SwiftRawDocumentReader : public RawDocumentReader
{
public:
    static const int64_t CONNECT_TIMEOUT_INTERVAL = 60 * 1000 * 1000; // 60s
    static const int64_t DEFAULT_WAIT_READER_TIME = 500 * 1000; //500ms
public:
    SwiftRawDocumentReader(const util::SwiftClientCreatorPtr& swiftClientCreator);
    ~SwiftRawDocumentReader();
private:
    SwiftRawDocumentReader(const SwiftRawDocumentReader &);
    SwiftRawDocumentReader& operator=(const SwiftRawDocumentReader &);
public:
    /* override */ bool init(const ReaderInitParam &params);
    /* override */ bool seek(int64_t offset);
    /* override */ bool isEof() const;
    /* override */ void suspendReadAtTimestamp(int64_t timestamp, ExceedTsAction action);
    bool isExceedSuspendTimestamp() const override { return _exceedSuspendTimestamp; } 
protected:
    ErrorCode readDocStr(std::string &docStr, int64_t &offset, int64_t &timestamp) override;
    ErrorCode readDocStr(std::vector<Message> &msgs, int64_t &offset, size_t maxMessageCount) override;
    /* override */ int64_t getFreshness();

public:
    // virtual for test
    virtual bool getMaxTimestamp(int64_t &timestamp);
public:
    bool getMaxTimestampAfterStartTimestamp(int64_t &timestamp);
protected:
    virtual SWIFT_NS(client)::SwiftClient *createSwiftClient(
            const std::string& zkPath,
            const std::string& swiftConfig);
    virtual SwiftReaderWrapper *createSwiftReader(
            const std::string &swiftReaderConfig, SWIFT_NS(protocol)::ErrorInfo *errorInfo);
    std::string createSwiftReaderConfig(const ReaderInitParam &params);
    
private:
    bool doSeek(int64_t timestamp);
    bool initSwiftReader(SwiftReaderWrapper *swiftReader,
                         const KeyValueMap &kvMap);
    bool setLimitTime(SwiftReaderWrapper *swiftReader,
                      const KeyValueMap &kvMap,
                      const std::string &limitTimeType);
    
    void setErrorCode(SWIFT_NS(protocol)::ErrorCode errorCode);
private:
    SwiftReaderWrapper *_swiftReader;
    SWIFT_NS(client)::SwiftClient *_swiftClient;
    volatile int64_t _lastDocTimestamp;
    IE_NAMESPACE(misc)::MetricPtr _swiftReadDelayMetric;
    IE_NAMESPACE(util)::StateCounterPtr _swiftReadDelayCounter;
    util::SwiftClientCreatorPtr _swiftClientCreator;
    int64_t _startTimestamp;
    ExceedTsAction _exceedTsAction;
    bool _exceedSuspendTimestamp;
    mutable autil::ThreadMutex _mutex;


private:
    friend class TestWorkItem;
    friend class SwiftRawDocumentReaderTest;
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(SwiftRawDocumentReader);

}
}

#endif //ISEARCH_BS_SWIFTRAWDOCUMENTREADER_H
