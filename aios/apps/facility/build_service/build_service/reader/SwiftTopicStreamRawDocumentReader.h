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

#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "build_service/reader/SwiftRawDocumentReader.h"
#include "build_service/util/Log.h"

namespace swift::client {
class SwiftReader;
}

namespace build_service::reader {

class SwiftTopicStreamRawDocumentReader : public SwiftRawDocumentReader
{
public:
    SwiftTopicStreamRawDocumentReader(const util::SwiftClientCreatorPtr& swiftClientCreator);
    ~SwiftTopicStreamRawDocumentReader();

public:
    bool init(const ReaderInitParam& params) override;

private:
    SwiftReaderWrapper* createSwiftReader(const ReaderInitParam& params) override;

private:
    static const std::string TOPIC_LIST;

private:
    BS_LOG_DECLARE();
};

} // namespace build_service::reader
