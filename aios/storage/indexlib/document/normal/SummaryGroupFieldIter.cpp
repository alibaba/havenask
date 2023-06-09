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
#include "indexlib/document/normal/SummaryGroupFieldIter.h"

#include "indexlib/index/summary/config/SummaryConfig.h"

using namespace std;
using namespace autil;

using namespace indexlib::config;

namespace indexlib { namespace document {
AUTIL_LOG_SETUP(indexlib.document, SummaryGroupFieldIter);

SummaryGroupFieldIter::SummaryGroupFieldIter(const std::shared_ptr<SummaryDocument>& document,
                                             const std::shared_ptr<indexlibv2::config::SummaryGroupConfig>& config)
    : _document(document)
    , _config(config)
    , _configIter(config->Begin())
{
}

SummaryGroupFieldIter::~SummaryGroupFieldIter() {}

bool SummaryGroupFieldIter::HasNext() const { return _configIter != _config->End(); }

const autil::StringView SummaryGroupFieldIter::Next()
{
    fieldid_t fieldId = (*_configIter)->GetFieldId();
    const StringView fieldValue = _document->GetField(fieldId);
    ++_configIter;
    return fieldValue;
}
}} // namespace indexlib::document
