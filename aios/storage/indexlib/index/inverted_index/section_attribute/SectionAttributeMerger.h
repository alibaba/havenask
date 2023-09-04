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
#include "indexlib/index/attribute/merger/MultiValueAttributeMerger.h"

namespace indexlib::index {

class SectionAttributeMerger : public indexlibv2::index::MultiValueAttributeMerger<char>
{
public:
    SectionAttributeMerger();
    ~SectionAttributeMerger();

public:
    std::pair<Status, std::shared_ptr<indexlibv2::index::IIndexer>>
    GetIndexerFromSegment(const std::shared_ptr<indexlibv2::framework::Segment>& segment,
                          const std::shared_ptr<indexlibv2::index::AttributeConfig>& attrConfig) override;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
