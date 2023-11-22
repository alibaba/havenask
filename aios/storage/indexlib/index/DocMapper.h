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
    static std::string GetDocMapperType() { return "DOC_MAPPER"; }

public:
    virtual int64_t GetTargetSegmentDocCount(segmentid_t segmentId) const = 0;
    virtual std::pair<segmentid_t, docid32_t> Map(docid64_t oldDocId) const = 0;
    virtual std::pair<segmentid_t, docid32_t> ReverseMap(docid64_t newDocId) const = 0;

    // oldGlobalDocId -> newDocId
    virtual docid64_t GetNewId(docid64_t oldId) const = 0;

    // new total doc count
    // TODO(xiaohao.yxh) 应该用size_t, 因为暂时只有bitmap用就还没改
    virtual uint32_t GetNewDocCount() const = 0;
    virtual segmentid_t GetLocalId(docid64_t newId) const = 0;
};

} // namespace indexlibv2::index
