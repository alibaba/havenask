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
#include "indexlib/index/inverted_index/builtin_index/adaptive_bitmap/AdaptiveBitmapTriggerCreator.h"

namespace indexlibv2::config {
class InvertedIndexConfig;
}

namespace indexlibv2::framework {
struct SegmentMeta;
} // namespace indexlibv2::framework

namespace indexlib::file_system {
class RelocatableFolder;
class IDirectory;
class IOConfig;
} // namespace indexlib::file_system
namespace indexlibv2::index {
class DocMapper;
}
namespace indexlib::index {

class MultiAdaptiveBitmapIndexWriter;
class AdaptiveBitmapTrigger;

class AdaptiveBitmapIndexWriterCreator
{
public:
    AdaptiveBitmapIndexWriterCreator(
        const std::shared_ptr<file_system::RelocatableFolder>& adaptiveDictFolder,
        const std::shared_ptr<indexlibv2::index::DocMapper>& docMapper,
        const std::vector<std::shared_ptr<indexlibv2::framework::SegmentMeta>>& targetSegments);
    virtual ~AdaptiveBitmapIndexWriterCreator() = default;

public:
    std::shared_ptr<MultiAdaptiveBitmapIndexWriter>
    Create(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConf,
           const file_system::IOConfig& ioConfig);

private:
    virtual std::shared_ptr<AdaptiveBitmapTrigger>
    CreateAdaptiveBitmapTrigger(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConf);

    std::shared_ptr<file_system::RelocatableFolder> _adaptiveDictFolder;
    AdaptiveBitmapTriggerCreator _triggerCreator;
    std::vector<std::shared_ptr<indexlibv2::framework::SegmentMeta>> _targetSegments;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
