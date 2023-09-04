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
#include "autil/mem_pool/Pool.h"
#include "indexlib/base/Constant.h"
#include "indexlib/base/Status.h"
#include "indexlib/base/Types.h"
#include "indexlib/config/CompressTypeOption.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/util/SimplePool.h"
#include "indexlib/util/buffer_compressor/BufferCompressorCreator.h"

namespace indexlib::util {
class BuildResourceMetrics;
class BuildResourceMetricsNode;
} // namespace indexlib::util

namespace indexlib::file_system {
class IDirectory;
}

namespace indexlibv2::index {
class BuildingIndexMemoryUseUpdater;

// Attribute updater keeps an in memory data structure(e.g. hashmap) that can be easily updated and use that to keep
// attribute values. When dump a more compact data structure will be created.
class AttributeUpdater : private autil::NoCopyable
{
public:
    AttributeUpdater(segmentid_t segId, const std::shared_ptr<config::IIndexConfig>& indexConfig);
    virtual ~AttributeUpdater() = default;

public:
    static const size_t DEFAULT_COMPRESS_BUFF_SIZE = 10 * 1024 * 1024; // 10MB

public:
    virtual void Update(docid_t docId, const autil::StringView& attributeValue, bool isNull) = 0;
    virtual Status Dump(const std::shared_ptr<indexlib::file_system::IDirectory>& attributeDir,
                        segmentid_t srcSegment) = 0;
    virtual void UpdateMemUse(BuildingIndexMemoryUseUpdater* memUpdater) = 0;

    std::pair<Status, std::shared_ptr<indexlib::file_system::FileWriter>>
    CreatePatchFileWriter(const std::shared_ptr<indexlib::file_system::IDirectory>& directory,
                          const std::string& fileName);

    int64_t EstimateDumpFileSize(int64_t valueSize) const
    {
        if (!_attrConfig->GetCompressType().HasPatchCompress()) {
            return valueSize;
        }
        return valueSize * indexlib::config::CompressTypeOption::PATCH_COMPRESS_RATIO;
    }

    int64_t GetPatchFileWriterBufferSize() const
    {
        if (!_attrConfig->GetCompressType().HasPatchCompress()) {
            return 0;
        }
        // comperssor inbuffer + out buffer
        return DEFAULT_COMPRESS_BUFF_SIZE * 2;
    }

public:
    const std::string& GetPatchFileName() const { return _patchFileName; }
    const std::string& GetPatchDirectory() const { return _patchDirectory; }

protected:
    // inline
    std::string ComputePatchFileName(segmentid_t srcSegment) const
    {
        std::stringstream ss;
        if (srcSegment == INVALID_SEGMENTID) {
            // TODO delete : support legacy patch name
            ss << _segmentId << "." << PATCH_FILE_NAME;
        } else {
            ss << srcSegment << "_" << _segmentId << "." << PATCH_FILE_NAME;
        }
        return ss.str();
    }

protected:
    segmentid_t _segmentId;
    std::shared_ptr<AttributeConfig> _attrConfig;
    std::string _patchDirectory;
    std::string _patchFileName;
    indexlib::util::SimplePool _simplePool;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
