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
#include <stdlib.h>

#include "indexlib/base/Status.h"
#include "indexlib/index/common/DictKeyInfo.h"

namespace indexlib::file_system {
class FileReader;
}
namespace indexlib::index {

class TruncateMetaReader
{
public:
    using Range = std::pair<int64_t, int64_t>;

public:
    TruncateMetaReader(bool desc);
    virtual ~TruncateMetaReader();

public:
    Status Open(const std::shared_ptr<file_system::FileReader>& file);
    bool IsExist(const index::DictKeyInfo& key);

    size_t Size() { return _dict.size(); }
    virtual bool Lookup(const index::DictKeyInfo& key, int64_t& min, int64_t& max) const;

private:
    Status AddOneRecord(const std::string& line);

protected:
    bool _desc;
    std::map<index::DictKeyInfo, Range> _dict;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
