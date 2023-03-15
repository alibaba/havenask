#ifndef ISEARCH_BS_KAFKARAWDOCUMENTREADER_H
#define ISEARCH_BS_KAFKARAWDOCUMENTREADER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/reader/RawDocumentReader.h"
#include "KafkaReaderWrapper.h"

namespace pluginplatform {
namespace reader_plugins {

class KafkaRawDocumentReader : public build_service::reader::RawDocumentReader
{
public:
    static const int64_t CONNECT_TIMEOUT_INTERVAL = 60 * 1000 * 1000; // 60s
    static const int64_t DEFAULT_WAIT_READER_TIME = 500 * 1000; //500ms
public:
    KafkaRawDocumentReader();
    ~KafkaRawDocumentReader();
private:
    KafkaRawDocumentReader(const KafkaRawDocumentReader &);
    KafkaRawDocumentReader& operator=(const KafkaRawDocumentReader &);
public:
    /* override */ bool init(const build_service::reader::ReaderInitParam &params);
    /* override */ bool seek(int64_t offset);
    /* override */ bool isEof() const;
    /* override */ void suspendReadAtTimestamp(int64_t timestamp, ExceedTsAction action);
    /* override */ int64_t getFreshness();
    bool isExceedSuspendTimestamp() const override { return _exceedSuspendTimestamp; }
protected:
    ErrorCode readDocStr(std::string &docStr, int64_t &offset, int64_t &timestamp) override;
    ErrorCode readDocStr(std::vector<Message> &msgs, int64_t &offset, size_t maxMessageCount) override;
public:
    bool getMaxTimestampAfterStartTimestamp(int64_t &timestamp);
    bool isStreamReader() { return true; }
private:
    bool doSeek(int64_t timestamp);
    bool initKafkaReader(KafkaReaderWrapperPtr &kafkaReader, const build_service::KeyValueMap &kvMap);
    bool setLimitTime(KafkaReaderWrapperPtr &kafkaReader,
                      const build_service::KeyValueMap &kvMap,
                      const std::string &limitTimeType);
private:
    KafkaReaderWrapperPtr _kafkaReader;
    volatile int64_t _lastDocTimestamp;
    IE_NAMESPACE(util)::StateCounterPtr _kafkaReadDelayCounter;
    int64_t _startTimestamp;
    ExceedTsAction _exceedTsAction;
    bool _exceedSuspendTimestamp;
    mutable autil::ThreadMutex _mutex;
private:
    BS_LOG_DECLARE();
};

typedef std::shared_ptr<KafkaRawDocumentReader> KafkaRawDocumentReaderPtr;

}
}

#endif //ISEARCH_BS_KAFKARAWDOCUMENTREADER_H
