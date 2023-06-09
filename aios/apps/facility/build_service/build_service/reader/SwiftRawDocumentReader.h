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
#ifndef ISEARCH_BS_SWIFTRAWDOCUMENTREADER_H
#define ISEARCH_BS_SWIFTRAWDOCUMENTREADER_H

#include "build_service/common/SwiftLinkFreshnessReporter.h"
#include "build_service/common/SwiftParam.h"
#include "build_service/common_define.h"
#include "build_service/reader/RawDocumentReader.h"
#include "build_service/reader/SwiftReaderWrapper.h"
#include "build_service/util/Log.h"
#include "build_service/util/SwiftClientCreator.h"

namespace indexlib { namespace util {
class Metric;
typedef std::shared_ptr<Metric> MetricPtr;
}} // namespace indexlib::util

namespace build_service { namespace reader {

class SwiftRawDocumentReader : public RawDocumentReader
{
public:
    static const int64_t CONNECT_TIMEOUT_INTERVAL = 60 * 1000 * 1000; // 60s
    static const int64_t DEFAULT_WAIT_READER_TIME = 500 * 1000;       // 500ms
public:
    SwiftRawDocumentReader(const util::SwiftClientCreatorPtr& swiftClientCreator);
    ~SwiftRawDocumentReader();

private:
    SwiftRawDocumentReader(const SwiftRawDocumentReader&);
    SwiftRawDocumentReader& operator=(const SwiftRawDocumentReader&);

public:
    bool init(const ReaderInitParam& params) override;
    bool seek(const Checkpoint& checkpoint) override;
    bool isEof() const override;
    void suspendReadAtTimestamp(int64_t timestamp, common::ExceedTsAction action) override;
    bool isExceedSuspendTimestamp() const override { return _exceedSuspendTimestamp; }

protected:
    ErrorCode readDocStr(std::string& docStr, Checkpoint* checkpoint, DocInfo& docInfo) override;
    ErrorCode readDocStr(std::vector<Message>& msgs, Checkpoint* checkpoint, size_t maxMessageCount) override;

public:
    // virtual for test
    virtual bool getMaxTimestamp(int64_t& timestamp);
    virtual int64_t getFreshness();

public:
    bool getMaxTimestampAfterStartTimestamp(int64_t& timestamp) override;
    bool getLastMsgTimestamp() const { return _lastMsgTimestamp; }
    bool getLastMsgId() const { return _lastMsgId; }
    bool isStreamReader() override { return true; }

protected:
    bool initMetrics(const ReaderInitParam& params);
    bool createSwiftClient(const ReaderInitParam& params);
    virtual SwiftReaderWrapper* createSwiftReader(const ReaderInitParam& params);
    virtual swift::client::SwiftClient* createSwiftClient(const std::string& zkPath, const std::string& swiftConfig);
    virtual SwiftReaderWrapper* createSwiftReader(const std::string& swiftReaderConfig,
                                                  swift::protocol::ErrorInfo* errorInfo);
    std::string createSwiftReaderConfig(const ReaderInitParam& params);

    virtual void reportTimestampFieldValue(int64_t value) override;
    virtual void doFillDocTags(document::RawDocument& rawDoc) override;

private:
    bool doSeek(int64_t timestamp, const std::vector<indexlibv2::base::Progress>& progress,
                const std::string& topicName);
    bool initSwiftReader(SwiftReaderWrapper* swiftReader, const KeyValueMap& kvMap);
    bool setLimitTime(SwiftReaderWrapper* swiftReader, const KeyValueMap& kvMap, const std::string& limitTimeType);

    void setErrorCode(swift::protocol::ErrorCode errorCode);
    bool initSwiftLinkReporter(const ReaderInitParam& params);

    void encodeSchemaDocString(const swift::protocol::Message& msg, std::string& docStr);

protected:
    SwiftReaderWrapper* _swiftReader;
    swift::client::SwiftClient* _swiftClient;
    volatile int64_t _lastDocTimestamp;
    volatile int64_t _noMoreMsgStartTs;
    indexlib::util::MetricPtr _swiftReadDelayMetric;
    indexlib::util::MetricPtr _msgTimeToCurDelayMetric;
    indexlib::util::StateCounterPtr _swiftReadDelayCounter;
    util::SwiftClientCreatorPtr _swiftClientCreator;
    common::SwiftLinkFreshnessReporterPtr _swiftLinkReporter;
    int64_t _startTimestamp;
    common::ExceedTsAction _exceedTsAction;
    common::SwiftParam _swiftParam;
    bool _exceedSuspendTimestamp;
    mutable autil::ThreadMutex _mutex;
    bool _isEof;
    bool _enableSchema;
    bool _enableRawTopicFastSlowQueue;
    volatile int64_t _lastMsgTimestamp;
    volatile int64_t _lastMsgId;
    volatile uint16_t _lastDocU16Payload;

private:
    friend class TestWorkItem;
    friend class SwiftRawDocumentReaderTest;
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(SwiftRawDocumentReader);

}} // namespace build_service::reader

#endif // ISEARCH_BS_SWIFTRAWDOCUMENTREADER_H
