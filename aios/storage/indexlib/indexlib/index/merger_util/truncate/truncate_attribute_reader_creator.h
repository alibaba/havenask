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

#include "indexlib/common_define.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(index, SegmentDirectoryBase);
DECLARE_REFERENCE_CLASS(index, ReclaimMap);
DECLARE_REFERENCE_CLASS(index::legacy, TruncateAttributeReader);
DECLARE_REFERENCE_CLASS(index, AttributeSegmentReader);
DECLARE_REFERENCE_CLASS(config, AttributeSchema);
DECLARE_REFERENCE_CLASS(config, AttributeConfig);

namespace indexlib::index::legacy {

class TruncateAttributeReaderCreator
{
public:
    TruncateAttributeReaderCreator(const SegmentDirectoryBasePtr& segmentDirectory,
                                   const config::AttributeSchemaPtr& attrSchema,
                                   const index_base::SegmentMergeInfos& segMergeInfos, const ReclaimMapPtr& reclaimMap);
    virtual ~TruncateAttributeReaderCreator();

public:
    virtual TruncateAttributeReaderPtr Create(const std::string& attrName);
    const config::AttributeSchemaPtr& GetAttributeSchema() const { return mAttrSchema; }
    const ReclaimMapPtr& GetReclaimMap() const { return mReclaimMap; }

private:
    TruncateAttributeReaderPtr CreateAttrReader(const std::string& fieldName);
    AttributeSegmentReaderPtr CreateAttrSegmentReader(const config::AttributeConfigPtr& attrConfig,
                                                      const index_base::SegmentMergeInfo& segMergeInfo);

private:
    template <typename T>
    AttributeSegmentReaderPtr CreateAttrSegmentReaderTyped(const config::AttributeConfigPtr& attrConfig,
                                                           const index_base::SegmentMergeInfo& segMergeInfo);

private:
    SegmentDirectoryBasePtr mSegmentDirectory;
    config::AttributeSchemaPtr mAttrSchema;
    index_base::SegmentMergeInfos mSegMergeInfos;
    ReclaimMapPtr mReclaimMap;
    std::map<std::string, TruncateAttributeReaderPtr> mTruncateAttributeReaders;
    autil::ThreadMutex mMutex;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TruncateAttributeReaderCreator);
} // namespace indexlib::index::legacy
