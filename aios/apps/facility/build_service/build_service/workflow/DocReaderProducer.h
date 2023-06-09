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
#ifndef ISEARCH_BS_DOCREADERPRODUCER_H
#define ISEARCH_BS_DOCREADERPRODUCER_H

#include "build_service/common_define.h"
#include "build_service/reader/RawDocumentReader.h"
#include "build_service/util/Log.h"
#include "build_service/workflow/Producer.h"

namespace build_service { namespace workflow {
class RawDocChecksumer;

class DocReaderProducer : public RawDocProducer
{
public:
    DocReaderProducer(const proto::PartitionId& pid, const config::ResourceReaderPtr& resourceReader,
                      reader::RawDocumentReader* reader, uint64_t srcSignature);
    virtual ~DocReaderProducer();

private:
    DocReaderProducer(const DocReaderProducer&);
    DocReaderProducer& operator=(const DocReaderProducer&);

public:
    FlowError produce(document::RawDocumentPtr& rawDoc) override;
    bool seek(const common::Locator& locator) override;
    bool stop(StopOption stopOption) override;

public:
    virtual bool getMaxTimestamp(int64_t& timestamp);
    virtual bool getLastReadTimestamp(int64_t& timestamp);
    uint64_t getLocatorSrc() const { return _srcSignature; }
    void setRecovered(bool isRecoverd);
    int64_t suspendReadAtTimestamp(int64_t timestamp, common::ExceedTsAction action) override
    {
        _reader->suspendReadAtTimestamp(timestamp, action);
        return 0;
    }

private:
    reader::RawDocumentReader* _reader;
    uint64_t _srcSignature;
    std::atomic<int64_t> _lastReadOffset;
    int64_t _ckpDocReportTime;
    int64_t _ckpDocReportInterval;
    std::shared_ptr<RawDocChecksumer> _checksum;

private:
    BS_LOG_DECLARE();
};

}} // namespace build_service::workflow

#endif // ISEARCH_BS_DOCREADERPRODUCER_H
