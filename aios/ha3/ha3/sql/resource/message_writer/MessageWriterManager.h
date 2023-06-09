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

#include "ha3/sql/common/Log.h"
#include "ha3/sql/resource/MessageWriter.h"

namespace isearch {
namespace sql {

class MessageWriterManager {
public:
    MessageWriterManager();
    virtual ~MessageWriterManager();
    MessageWriterManager(const MessageWriterManager &) = delete;
    MessageWriterManager &operator=(const MessageWriterManager &) = delete;

public:
    virtual bool init(const std::string &config) = 0;
    virtual MessageWriter *getMessageWriter(const std::string &dbName,
            const std::string &tableName) = 0;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<MessageWriterManager> MessageWriterManagerPtr;

} // end namespace sql
} // end namespace isearch
