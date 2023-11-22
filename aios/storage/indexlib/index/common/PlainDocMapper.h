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
#include "indexlib/index/IIndexMerger.h"

namespace indexlibv2::index {

class PlainDocMapper : public DocMapper
{
public:
    PlainDocMapper(const IIndexMerger::SegmentMergeInfos& segmentMergeInfos);
    ~PlainDocMapper();

public:
    int64_t GetTargetSegmentDocCount(segmentid_t segmentId) const override;
    std::pair<segmentid_t, docid32_t> Map(docid64_t oldDocId) const override;
    std::pair<segmentid_t, docid32_t> ReverseMap(docid64_t newDocId) const override;

    // oldGlobalDocId -> newDocId
    docid64_t GetNewId(docid64_t oldId) const override;

    // new total doc count
    uint32_t GetNewDocCount() const override;
    segmentid_t GetLocalId(docid64_t newId) const override;
    static std::string GetDocMapperName() { return "PlainDocMapper"; }
    Status Store(const std::shared_ptr<indexlib::file_system::Directory>& resourceDirectory) override;
    Status Load(const std::shared_ptr<indexlib::file_system::Directory>& resourceDirectory) override;

private:
    IIndexMerger::SegmentMergeInfos _segmentMergeInfos;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
