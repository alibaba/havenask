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
#include "indexlib/index/merger_util/truncate/truncate_meta_reader.h"

#include "autil/StringUtil.h"
#include "indexlib/file_system/archive/LineReader.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace autil;

using namespace indexlib::file_system;

namespace indexlib::index::legacy {
IE_LOG_SETUP(index, TruncateMetaReader);

TruncateMetaReader::TruncateMetaReader(bool desc) : mDesc(desc) {}

TruncateMetaReader::~TruncateMetaReader() {}

void TruncateMetaReader::Open(const file_system::FileReaderPtr& file)
{
    LineReader reader;
    reader.Open(file);
    string line;
    ErrorCode ec;
    while (reader.NextLine(line, ec)) {
        AddOneRecord(line);
    }
    THROW_IF_FS_ERROR(ec, "NextLine failed, file[%s]", file->DebugString().c_str());
}

void TruncateMetaReader::AddOneRecord(const string& line)
{
    vector<string> strVec = StringUtil::split(line, "\t");
    assert(strVec.size() == 2);

    index::DictKeyInfo key;
    int64_t value;
    key.FromString(strVec[0]);
    if (!StringUtil::fromString(strVec[1], value)) {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "invalid value: %s", strVec[1].c_str());
    }

    if (mDesc) {
        int64_t max = numeric_limits<int64_t>::max();
        mDict[key] = pair<dictkey_t, int64_t>(value, max);
    } else {
        int64_t min = numeric_limits<int64_t>::min();
        mDict[key] = pair<dictkey_t, int64_t>(min, value);
    }
}

bool TruncateMetaReader::IsExist(const index::DictKeyInfo& key) { return mDict.find(key) != mDict.end(); }

bool TruncateMetaReader::Lookup(const index::DictKeyInfo& key, int64_t& min, int64_t& max)
{
    DictType::iterator it = mDict.find(key);
    if (it == mDict.end()) {
        return false;
    }
    min = it->second.first;
    max = it->second.second;
    return true;
}
} // namespace indexlib::index::legacy
