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

#include "indexlib/index/common/DictKeyInfo.h"
#include "indexlib/index/inverted_index/Types.h"
#include "indexlib/index/inverted_index/truncate/TruncateMetaReader.h"

namespace indexlib::index {

struct TruncateTriggerInfo {
    TruncateTriggerInfo() : key(-1), df(0) {}
    TruncateTriggerInfo(DictKeyInfo _key, df_t _df) : key(_key), df(_df) {}
    const DictKeyInfo& GetDictKey() const { return key; }
    df_t GetDF() const { return df; }

    DictKeyInfo key;
    df_t df;
};

class TruncateTrigger
{
public:
    TruncateTrigger() = default;
    virtual ~TruncateTrigger() = default;

public:
    virtual bool NeedTruncate(const TruncateTriggerInfo& info) const = 0;
    virtual void SetTruncateMetaReader(const std::shared_ptr<TruncateMetaReader>& reader) {}

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
