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

#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "indexlib/base/Status.h"
#include "indexlib/base/Types.h"
#include "indexlib/index/common/data_structure/VarLenDataAccessor.h"

namespace indexlib::document {
class SearchSummaryDocument;
}

namespace indexlibv2::config {
class SummaryGroupConfig;
}

namespace indexlibv2::index {
class VarLenDataAccessor;
class SummaryMemReader : private autil::NoCopyable
{
public:
    SummaryMemReader(const std::shared_ptr<indexlibv2::config::SummaryGroupConfig>& summaryGroupConfig,
                     VarLenDataAccessor* accessor);
    ~SummaryMemReader();

public:
    std::pair<Status, bool> GetDocument(docid_t localDocId,
                                        indexlib::document::SearchSummaryDocument* summaryDoc) const;
    size_t GetDocCount() { return _accessor->GetDocCount(); }

private:
    std::shared_ptr<indexlibv2::config::SummaryGroupConfig> _summaryGroupConfig;
    VarLenDataAccessor* _accessor;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
