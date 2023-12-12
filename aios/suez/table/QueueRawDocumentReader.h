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

#include "autil/LockFreeQueue.h"
#include "build_service/reader/RawDocumentReader.h"
#include "suez/table/wal/GlobalQueueManager.h"

namespace suez {

class QueueRawDocumentReader : public build_service::reader::RawDocumentReader {
public:
    QueueRawDocumentReader();
    ~QueueRawDocumentReader();

private:
    QueueRawDocumentReader(const QueueRawDocumentReader &);
    QueueRawDocumentReader &operator=(const QueueRawDocumentReader &);

public:
    virtual bool initialize(const build_service::reader::ReaderInitParam &params);
    virtual ErrorCode readDocStr(std::string &docStr,
                                 build_service::reader::Checkpoint *checkpoint,
                                 build_service::reader::DocInfo &docInfo);
    virtual bool seek(const build_service::reader::Checkpoint &checkpoint);
    virtual bool isEof() const;

private:
    bool initQueue(const build_service::reader::ReaderInitParam &params);

private:
    const static std::string QUEUE_NAME;

private:
    std::shared_ptr<autil::LockFreeQueue<RawDoc>> _docQueuePtr;
    std::shared_ptr<autil::LockFreeQueue<RawDoc>> _docBackupQueuePtr;
    std::string _queueName;
    AUTIL_LOG_DECLARE();
};

} // namespace suez
