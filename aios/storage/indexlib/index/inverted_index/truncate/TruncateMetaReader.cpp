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
#include "indexlib/index/inverted_index/truncate/TruncateMetaReader.h"

#include "autil/StringUtil.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/archive/LineReader.h"
#include "indexlib/file_system/file/FileReader.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(index, TruncateMetaReader);

TruncateMetaReader::TruncateMetaReader(bool desc) : _desc(desc) {}

TruncateMetaReader::~TruncateMetaReader() {}

Status TruncateMetaReader::Open(const std::shared_ptr<file_system::FileReader>& file)
{
    file_system::LineReader reader;
    reader.Open(file);
    std::string line;
    file_system::ErrorCode ec;
    while (reader.NextLine(line, ec)) {
        RETURN_IF_STATUS_ERROR(AddOneRecord(line), "add one record failed");
    }
    if (ec != indexlib::file_system::FSEC_OK) {
        AUTIL_LOG(ERROR, "read next line failed. file[%s]", file->DebugString().c_str());
        return Status::InternalError();
    }
    return Status::OK();
}

Status TruncateMetaReader::AddOneRecord(const std::string& line)
{
    std::vector<std::string> strVec = autil::StringUtil::split(line, "\t");
    assert(strVec.size() == 2);

    index::DictKeyInfo key;
    int64_t value;
    key.FromString(strVec[0]);
    if (!autil::StringUtil::fromString(strVec[1], value)) {
        AUTIL_LOG(ERROR, "convert to number failed, str[%s]", strVec[1].c_str());
        return Status::InternalError();
    }

    if (_desc) {
        int64_t max = std::numeric_limits<int64_t>::max();
        _dict[key] = std::pair<dictkey_t, int64_t>(value, max);
    } else {
        int64_t min = std::numeric_limits<int64_t>::min();
        _dict[key] = std::pair<dictkey_t, int64_t>(min, value);
    }
    return Status::OK();
}

bool TruncateMetaReader::IsExist(const index::DictKeyInfo& key) { return _dict.find(key) != _dict.end(); }

bool TruncateMetaReader::Lookup(const index::DictKeyInfo& key, int64_t& min, int64_t& max) const
{
    auto it = _dict.find(key);
    if (it == _dict.end()) {
        return false;
    }
    min = it->second.first;
    max = it->second.second;
    return true;
}
} // namespace indexlib::index
