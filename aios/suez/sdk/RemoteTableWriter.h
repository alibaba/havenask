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

#include <functional>
#include <memory>

#include "autil/result/Result.h"

namespace multi_call {
class SearchService;
} // namespace multi_call

namespace suez {
struct MessageWriteParam;
} // namespace suez

namespace suez {

class WriteRequest;
class WriteResponse;

struct WriteCallbackParam;

class RemoteTableWriter : autil::NoCopyable {
public:
    RemoteTableWriter(const std::string &zoneName,
                      multi_call::SearchService *searchService,
                      bool allowFollowWrite = false);
    virtual ~RemoteTableWriter();

public:
    // virtual for ut
    virtual void write(const std::string &tableName, const std::string &format, suez::MessageWriteParam writeParam);

private:
    std::string _zoneName;
    multi_call::SearchService *_searchService = nullptr;
    bool _allowFollowWrite;
};

typedef std::shared_ptr<RemoteTableWriter> RemoteTableWriterPtr;

} // namespace suez
