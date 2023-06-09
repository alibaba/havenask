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

#include <string>

#include "autil/DynamicBuf.h"
#include "autil/StringUtil.h"
#include "autil/StringView.h"

namespace build_service::reader {

class RawDocumentBuilder
{
public:
    RawDocumentBuilder(const std::string& prefix, const std::string& suffix, const std::string& fieldSep,
                       const std::string& kvSep);
    ~RawDocumentBuilder();

public:
    template <typename T>
    void addField(const autil::StringView& key, const T& value)
    {
        auto valueStr = autil::StringUtil::toString(value);
        addImpl(key, valueStr);
    }

    autil::StringView finalize();

private:
    void addImpl(const autil::StringView& key, const autil::StringView& value);

private:
    const std::string _prefix;
    const std::string _suffix;
    const std::string _fieldSep;
    const std::string _kvSep;
    autil::DynamicBuf _buf;

private:
    static constexpr size_t DEFAULT_BUF_SIZE = 1024;
};

} // namespace build_service::reader
