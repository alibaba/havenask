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

#include "suez/table/wal/GlobalQueueManager.h"
#include "suez/table/wal/WALStrategy.h"

namespace suez {

class QueueWAL : public WALStrategy {
public:
    QueueWAL();
    ~QueueWAL();

private:
    QueueWAL(const QueueWAL &);
    QueueWAL &operator=(const QueueWAL &);

public:
    bool init(const WALConfig &config);
    void log(const std::vector<std::pair<uint16_t, std::string>> &strs, CallbackType done) override;
    void stop() override;
    void flush() override {}

private:
    const static std::string QUEUE_NAME;
    const static std::string QUEUE_SIZE;

private:
    uint32_t _maxQueueSize = 1000;
    std::string _queueName;
    RawDocQueue _docQueuePtr;
};

} // namespace suez
