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

#include <map>
#include <memory>

#include "suez/drc/Sink.h"

namespace build_service {
namespace util {
class SwiftClientCreator;
}
} // namespace build_service

namespace swift::client {
class SwiftWriter;
}

namespace suez {

struct SinkConfig;

struct SwiftSinkConfig {
    std::string swiftRoot;
    std::string topicName;
    std::string clientConfig;
    std::string writerConfig;

    bool init(const std::map<std::string, std::string> &params);
    std::string constructWriterConfigStr() const;
};

class SwiftSink : public Sink {
public:
    explicit SwiftSink(std::unique_ptr<swift::client::SwiftWriter> writer);
    ~SwiftSink();

public:
    const std::string &getType() const override;
    bool write(uint32_t hashId, std::string data, int64_t checkpoint) override;
    int64_t getCommittedCheckpoint() const override;

public:
    static std::unique_ptr<Sink> create(const std::shared_ptr<build_service::util::SwiftClientCreator> &client,
                                        const SinkConfig &config);

private:
    std::unique_ptr<swift::client::SwiftWriter> _writer;

public:
    static const std::string TYPE;
};

} // namespace suez
