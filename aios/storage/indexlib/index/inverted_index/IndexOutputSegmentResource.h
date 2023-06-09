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

#include <memory>

#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/fslib/IoConfig.h"
#include "indexlib/index/inverted_index/IndexDataWriter.h"
#include "indexlib/index/inverted_index/SegmentTermInfo.h"
#include "indexlib/util/SimplePool.h"

namespace indexlibv2::framework {
class SegmentStatistics;
}

namespace indexlibv2::config {
class InvertedIndexConfig;
}

namespace indexlib::index {

class IndexOutputSegmentResource
{
public:
    IndexOutputSegmentResource(size_t dictKeyCount = 0) : _dictKeyCount(dictKeyCount) {}
    ~IndexOutputSegmentResource();

    void Init(const file_system::DirectoryPtr& mergeDir,
              const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig,
              const file_system::IOConfig& ioConfig, const std::string& temperatureLayerStr,
              util::SimplePool* simplePool, bool hasAdaptiveBitMap);

    void Init(const file_system::DirectoryPtr& mergeDir,
              const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig,
              const file_system::IOConfig& ioConfig,
              const std::shared_ptr<indexlibv2::framework::SegmentStatistics>& segmentstatistics,
              util::SimplePool* simplePool, bool hasAdaptiveBitMap);

    void Reset();
    std::shared_ptr<IndexDataWriter>& GetIndexDataWriter(SegmentTermInfo::TermIndexMode mode);

private:
    void CreateNormalIndexDataWriter(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig,
                                     const file_system::IOConfig& IOConfig, util::SimplePool* simplePool);
    void CreateBitmapIndexDataWriter(const file_system::IOConfig& IOConfig, util::SimplePool* simplePool,
                                     bool hasAdaptiveBitMap);
    void DestroyIndexDataWriters()
    {
        FreeIndexDataWriter(_normalIndexDataWriter);
        FreeIndexDataWriter(_bitmapIndexDataWriter);
    }

private:
    void FreeIndexDataWriter(std::shared_ptr<IndexDataWriter>& writer)
    {
        if (writer) {
            if (writer->IsValid()) {
                writer->dictWriter->Close();
                writer->postingWriter->Close().GetOrThrow();
            }
            writer->dictWriter.reset();
            writer->postingWriter.reset();
            writer.reset();
        }
    }

private:
    file_system::DirectoryPtr _mergeDir;
    std::string _temperatureLayer;
    std::shared_ptr<indexlibv2::framework::SegmentStatistics> _segmentStatistics;
    std::shared_ptr<IndexDataWriter> _normalIndexDataWriter;
    std::shared_ptr<IndexDataWriter> _bitmapIndexDataWriter;
    size_t _dictKeyCount;

    friend class BitmapPostingMergerTest;
    friend class ExpackPostingMergerTest;
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<IndexOutputSegmentResource> IndexOutputSegmentResourcePtr;
typedef std::vector<IndexOutputSegmentResourcePtr> IndexOutputSegmentResources;
} // namespace indexlib::index
