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

#include "autil/mem_pool/Pool.h"
#include "indexlib/common_define.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/index_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/SimplePool.h"
#include "indexlib/util/buffer_compressor/BufferCompressorCreator.h"
#include "indexlib/util/memory_control/BuildResourceMetrics.h"

namespace indexlib { namespace index {

// Attribute updater keeps an in memory data structure(e.g. hashmap) that can be easily updated and use that to keep
// attribute values. When dump a more compact data structure will be created.
class AttributeUpdater
{
public:
    AttributeUpdater(util::BuildResourceMetrics* buildResourceMetrics, segmentid_t segId,
                     const config::AttributeConfigPtr& attrConfig)
        : mBuildResourceMetrics(buildResourceMetrics)
        , mBuildResourceMetricsNode(NULL)
        , mSegmentId(segId)
        , mAttrConfig(attrConfig)
    {
        if (mBuildResourceMetrics) {
            mBuildResourceMetricsNode = mBuildResourceMetrics->AllocateNode();
            IE_LOG(INFO, "allocate build resource node [id:%d] for AttributeUpdater[%s] in segment[%d]",
                   mBuildResourceMetricsNode->GetNodeId(), mAttrConfig->GetAttrName().c_str(), segId);
        }
    }
    virtual ~AttributeUpdater() {}

public:
    static const size_t DEFAULT_COMPRESS_BUFF_SIZE = 10 * 1024 * 1024; // 10MB

public:
    virtual void Update(docid_t docId, const autil::StringView& attributeValue, bool isNull = false) = 0;
    virtual void Dump(const file_system::DirectoryPtr& attributeDir, segmentid_t srcSegment) = 0;
    std::shared_ptr<file_system::FileWriter> CreatePatchFileWriter(const file_system::DirectoryPtr& directory,
                                                                   const std::string& fileName);
    int64_t EstimateDumpFileSize(int64_t valueSize)
    {
        if (!mAttrConfig->GetCompressType().HasPatchCompress()) {
            return valueSize;
        }
        return valueSize * config::CompressTypeOption::PATCH_COMPRESS_RATIO;
    }

    int64_t GetPatchFileWriterBufferSize()
    {
        if (!mAttrConfig->GetCompressType().HasPatchCompress()) {
            return 0;
        }
        // comperssor inbuffer + out buffer
        return DEFAULT_COMPRESS_BUFF_SIZE * 2;
    }

protected:
    std::string GetPatchFileName(segmentid_t srcSegment) const;

protected:
    util::BuildResourceMetrics* mBuildResourceMetrics;
    util::BuildResourceMetricsNode* mBuildResourceMetricsNode;
    segmentid_t mSegmentId;
    config::AttributeConfigPtr mAttrConfig;
    util::SimplePool mSimplePool;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AttributeUpdater);
//////////////////////////////////////////////////////////////////

inline std::string AttributeUpdater::GetPatchFileName(segmentid_t srcSegment) const
{
    std::stringstream ss;
    if (srcSegment == INVALID_SEGMENTID) {
        // TODO delete : support legacy patch name
        ss << mSegmentId << "." << PATCH_FILE_NAME;
    } else {
        ss << srcSegment << "_" << mSegmentId << "." << PATCH_FILE_NAME;
    }
    return ss.str();
}
}} // namespace indexlib::index
