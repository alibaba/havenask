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
#include <stdlib.h>

#include "indexlib/common_define.h"
#include "indexlib/index/common/DictKeyInfo.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(file_system, FileReader);

namespace indexlib::index::legacy {

class TruncateMetaReader
{
public:
    typedef std::pair<int64_t, int64_t> Range;
    typedef std::map<index::DictKeyInfo, Range> DictType;

public:
    TruncateMetaReader(bool desc);
    virtual ~TruncateMetaReader();

public:
    void Open(const file_system::FileReaderPtr& archiveFile);
    bool IsExist(const index::DictKeyInfo& key);

    size_t Size() { return mDict.size(); }
    virtual bool Lookup(const index::DictKeyInfo& key, int64_t& min, int64_t& max);

private:
    void AddOneRecord(const std::string& line);

public:
    // for test
    void SetDict(const DictType& dict) { mDict = dict; }

protected:
    bool mDesc;
    DictType mDict;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TruncateMetaReader);
} // namespace indexlib::index::legacy
