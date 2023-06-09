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

#include "indexlib/base/Types.h"
#include "indexlib/framework/index_task/IndexTaskResource.h"

namespace indexlibv2::index {

class DocMapper : public framework::IndexTaskResource
{
public:
    DocMapper(std::string name, framework::IndexTaskResourceType type)
        : framework::IndexTaskResource(std::move(name), type)
    {
    }
    virtual ~DocMapper() {}

    virtual std::pair<segmentid_t, docid_t> Map(docid_t oldDocId) const = 0;
    virtual std::pair<segmentid_t, docid_t> ReverseMap(docid_t newDocId) const = 0;
    virtual segmentid_t GetTargetSegmentId(int32_t targetSegmentIdx) const = 0;
    virtual int64_t GetTargetSegmentDocCount(int32_t idx) const = 0;
    // oldGlobalDocId -> newDocId
    virtual docid_t GetNewId(docid_t oldId) const = 0;

    // new total doc count
    virtual uint32_t GetNewDocCount() const = 0;
    virtual segmentid_t GetLocalId(docid_t newId) const = 0;
    virtual segmentid_t GetTargetSegmentIndex(docid_t newId) const = 0;

    static std::string GetDocMapperType() { return "DOC_MAPPER"; }
};

} // namespace indexlibv2::index
