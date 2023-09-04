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

#include <memory>
#include <string>
#include <unordered_map>

#include "autil/Log.h"
#include "sql/resource/MessageWriterManager.h"
#include "sql/resource/SwiftMessageWriter.h"
#include "swift/client/SwiftClient.h"

namespace sql {
class MessageWriter;

class SwiftMessageWriterManager : public MessageWriterManager {
public:
    SwiftMessageWriterManager();
    ~SwiftMessageWriterManager();
    SwiftMessageWriterManager(const SwiftMessageWriterManager &) = delete;
    SwiftMessageWriterManager &operator=(const SwiftMessageWriterManager &) = delete;

public:
    bool init(const std::string &config) override;
    MessageWriter *getMessageWriter(const std::string &dbName,
                                    const std::string &tableName) override;

private:
    // for test
    virtual void createSwiftClient();

private:
    void reset();

private:
    swift::client::SwiftClientPtr _swiftClient;
    std::unordered_map<std::string, SwiftMessageWriterPtr> _swiftWriterMap;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<SwiftMessageWriterManager> SwiftMessageWriterManagerPtr;

} // namespace sql
