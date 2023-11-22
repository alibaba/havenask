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
#include "indexlib/index/DocMapper.h"

namespace indexlib::index {

class SimpleDocMapper : public indexlibv2::index::DocMapper
{
public:
    SimpleDocMapper(segmentid_t targetSegmentId, uint32_t totalDocCount)
        : DocMapper("SimpleDocMapper", indexlibv2::index::DocMapper::GetDocMapperType())
        , _targetSegmentId(targetSegmentId)
        , _totalDocCount(totalDocCount)
    {
    }
    ~SimpleDocMapper() {};

public:
    int64_t GetTargetSegmentDocCount(segmentid_t segmentId) const override { return _totalDocCount; }
    std::pair<segmentid_t, docid32_t> Map(docid64_t oldDocId) const override { return {_targetSegmentId, oldDocId}; };
    std::pair<segmentid_t, docid32_t> ReverseMap(docid64_t newDocId) const override
    {
        return {_targetSegmentId, newDocId};
    }
    docid64_t GetNewId(docid64_t oldId) const override { return oldId; }
    uint32_t GetNewDocCount() const override { return _totalDocCount; }

    Status Store(const std::shared_ptr<indexlib::file_system::Directory>& resourceDirectory) override
    {
        return Status::Unimplement();
    }
    Status Load(const std::shared_ptr<indexlib::file_system::Directory>& resourceDirectory) override
    {
        return Status::Unimplement();
    }
    segmentid_t GetLocalId(docid64_t newId) const override { return -1; }

private:
    segmentid_t _targetSegmentId;
    uint32_t _totalDocCount;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
