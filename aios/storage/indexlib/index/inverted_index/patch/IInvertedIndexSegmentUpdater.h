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
#include "indexlib/document/normal/ModifiedTokens.h"
#include "indexlib/index/common/DictKeyInfo.h"

namespace indexlib::file_system {
class IDirectory;
}
namespace indexlibv2::config {
class InvertedIndexConfig;
}

namespace indexlib::index {
class IInvertedIndexSegmentUpdater : private autil::NoCopyable
{
public:
    static std::string GetPatchFileName(segmentid_t srcSegment, segmentid_t destSegment);

public:
    IInvertedIndexSegmentUpdater(segmentid_t segId,
                                 const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig);
    virtual ~IInvertedIndexSegmentUpdater() = default;

public:
    virtual void Update(docid_t docId, const document::ModifiedTokens& modifiedTokens) = 0;
    virtual void Update(docid_t localDocId, DictKeyInfo termKey, bool isDelete) = 0;
    virtual Status Dump(const std::shared_ptr<file_system::IDirectory>& indexesDir, segmentid_t srcSegment) = 0;

protected:
    segmentid_t _segmentId;
    std::shared_ptr<indexlibv2::config::InvertedIndexConfig> _indexConfig;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
