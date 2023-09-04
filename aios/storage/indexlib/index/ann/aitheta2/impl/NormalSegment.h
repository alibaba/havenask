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
#include "indexlib/index/ann/aitheta2/CommonDefine.h"
#include "indexlib/index/ann/aitheta2/impl/DocMapWrapperBase.h"
#include "indexlib/index/ann/aitheta2/impl/Segment.h"

namespace indexlibv2::index::ann {

class NormalSegment : public Segment
{
public:
    NormalSegment(const indexlib::file_system::DirectoryPtr& directory, const AithetaIndexConfig& indexConfig,
                  bool isOnline)
        : Segment(indexConfig, SegmentType::ST_NORMAL, isOnline)
    {
        SetDirectory(directory);
    }

    ~NormalSegment() = default;

public:
    bool Open();
    bool DumpSegmentMeta();
    void SetDocMapWrapper(const std::shared_ptr<DocMapWrapperBase>& docIdMapWrapper)
    {
        _docMapWrapper = docIdMapWrapper;
    }
    const std::shared_ptr<DocMapWrapperBase>& GetDocMapWrapper() const { return _docMapWrapper; }

public:
    static size_t EstimateLoadMemoryUse(const indexlib::file_system::DirectoryPtr& parentDir);
    static bool GetDocCountInfo(const indexlib::file_system::DirectoryPtr& parentDir, DocCountMap& docCountMap);

private:
    bool IsServiceable() const;

protected:
    std::shared_ptr<DocMapWrapperBase> _docMapWrapper;

    AUTIL_LOG_DECLARE();
};
typedef std::shared_ptr<NormalSegment> NormalSegmentPtr;
using NormalSegmentVector = std::vector<NormalSegmentPtr>;

} // namespace indexlibv2::index::ann
