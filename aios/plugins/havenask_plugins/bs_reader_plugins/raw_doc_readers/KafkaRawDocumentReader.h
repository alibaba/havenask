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
#ifndef ISEARCH_BS_KAFKARAWDOCUMENTREADER_H
#define ISEARCH_BS_KAFKARAWDOCUMENTREADER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/reader/RawDocumentReader.h"
#include "KafkaReaderWrapper.h"
#include "build_service/common/ExceedTsAction.h"

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
    bool init(const build_service::reader::ReaderInitParam &params) override;
    bool seek(const build_service::reader::Checkpoint& checkpoint) override;
    bool isEof() const override;
    void suspendReadAtTimestamp(int64_t timestamp, build_service::common::ExceedTsAction action) override;
    int64_t getFreshness();
    bool isExceedSuspendTimestamp() const override { return _exceedSuspendTimestamp; }
protected:
    ErrorCode readDocStr(std::string &docStr, build_service::reader::Checkpoint* checkpoint, build_service::reader::DocInfo& docInfo) override;
    ErrorCode readDocStr(std::vector<Message> &msgs, build_service::reader::Checkpoint* checkpoint, size_t maxMessageCount) override;
public:
    bool getMaxTimestampAfterStartTimestamp(int64_t &timestamp) override;
    bool isStreamReader() override { return true; }
    bool seek(int64_t offset);
private:
    bool initKafkaReader(KafkaReaderWrapperPtr &kafkaReader, const build_service::KeyValueMap &kvMap);
    bool setLimitTime(KafkaReaderWrapperPtr &kafkaReader,
                      const build_service::KeyValueMap &kvMap,
                      const std::string &limitTimeType);
private:
    KafkaReaderWrapperPtr _kafkaReader;
    volatile int64_t _lastDocTimestamp;
    indexlib::util::StateCounterPtr _kafkaReadDelayCounter;
    int64_t _startTimestamp;
    build_service::common::ExceedTsAction _exceedTsAction;
    bool _exceedSuspendTimestamp;
    mutable autil::ThreadMutex _mutex;
private:
    BS_LOG_DECLARE();
};

typedef std::shared_ptr<KafkaRawDocumentReader> KafkaRawDocumentReaderPtr;

}
}

#endif //ISEARCH_BS_KAFKARAWDOCUMENTREADER_H
