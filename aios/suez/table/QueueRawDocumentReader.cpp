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
#include "suez/table/QueueRawDocumentReader.h"

#include <autil/Log.h>

#include "suez/table/wal/GlobalQueueManager.h"

using namespace std;
using namespace build_service;
using namespace build_service::reader;

namespace suez {

AUTIL_LOG_SETUP(suez, QueueRawDocumentReader);

const string QueueRawDocumentReader::QUEUE_NAME = "queue_name";

QueueRawDocumentReader::QueueRawDocumentReader() : RawDocumentReader() {}

QueueRawDocumentReader::~QueueRawDocumentReader() {}

bool QueueRawDocumentReader::initialize(const ReaderInitParam &params) {
    if (!RawDocumentReader::initialize(params)) {
        return false;
    }
    if (!initQueue(params.kvMap)) {
        return false;
    }
    AUTIL_LOG(INFO, "create queue raw document reader success, queue_name[%s]", _queueName.c_str());
    return true;
}

bool QueueRawDocumentReader::initQueue(const KeyValueMap &kvMap) {
    _queueName = getValueFromKeyValueMap(kvMap, QUEUE_NAME);
    if (_queueName.empty()) {
        AUTIL_LOG(ERROR, "queue name is empty for queue_wal.");
        return false;
    }
    _docQueuePtr = GlobalQueueManager::getInstance()->createQueue(_queueName);
    if (_docQueuePtr == nullptr) {
        AUTIL_LOG(ERROR, "get queue failed, queue_name[%s]", _queueName.c_str());
        return false;
    }
    return true;
}

RawDocumentReader::ErrorCode
QueueRawDocumentReader::readDocStr(string &docStr, Checkpoint *checkpoint, DocInfo &docInfo) {
    if (!_docQueuePtr) {
        return RawDocumentReader::ERROR_EXCEPTION;
    }
    if (_docQueuePtr->Empty()) {
        return RawDocumentReader::ERROR_WAIT;
    }
    RawDoc doc;
    if (_docQueuePtr->Pop(&doc)) {
        docStr = doc.second;
        checkpoint->offset = doc.first;
        vector<indexlibv2::base::Progress>progress;
        progress.emplace_back(indexlibv2::base::Progress({doc.first + 1, 0}));
        docInfo.hashId = 0;
        docInfo.timestamp = doc.first;
        checkpoint->progress = {progress};
        return RawDocumentReader::ERROR_NONE;
    }
    return RawDocumentReader::ERROR_WAIT;
}

bool QueueRawDocumentReader::seek(const Checkpoint &checkpoint) { return true; }

bool QueueRawDocumentReader::isEof() const { return _docQueuePtr->Empty(); }

} // namespace suez
