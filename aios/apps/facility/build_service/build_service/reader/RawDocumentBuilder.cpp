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
#include "build_service/reader/RawDocumentBuilder.h"

namespace build_service::reader {

RawDocumentBuilder::RawDocumentBuilder(const std::string& prefix, const std::string& suffix,
                                       const std::string& fieldSep, const std::string& kvSep)
    : _prefix(prefix)
    , _suffix(suffix)
    , _fieldSep(fieldSep)
    , _kvSep(kvSep)
    , _buf(DEFAULT_BUF_SIZE)
{
    if (!_prefix.empty()) {
        _buf.add(_prefix.c_str(), _prefix.size());
    }
}

RawDocumentBuilder::~RawDocumentBuilder() {}

autil::StringView RawDocumentBuilder::finalize()
{
    if (!_suffix.empty()) {
        _buf.add(_suffix.c_str(), _suffix.size());
    }
    return autil::StringView(_buf.getBuffer(), _buf.getDataLen());
}

void RawDocumentBuilder::addImpl(const autil::StringView& key, const autil::StringView& value)
{
    _buf.add(key.data(), key.size());
    _buf.add(_kvSep.c_str(), _kvSep.size());
    _buf.add(value.data(), value.size());
    _buf.add(_fieldSep.c_str(), _fieldSep.size());
}

} // namespace build_service::reader
