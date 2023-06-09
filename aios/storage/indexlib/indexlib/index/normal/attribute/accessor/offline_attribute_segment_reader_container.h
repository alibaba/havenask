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
#ifndef __INDEXLIB_OFFLINE_ATTRIBUTE_SEGMENT_READER_CONTAINER_H
#define __INDEXLIB_OFFLINE_ATTRIBUTE_SEGMENT_READER_CONTAINER_H

#include <memory>
#include <unordered_map>

#include "autil/Lock.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(index, SegmentDirectoryBase);
DECLARE_REFERENCE_CLASS(index, AttributeSegmentReader);
DECLARE_REFERENCE_CLASS(index_base, SegmentData);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(config, AttributeConfig);

namespace indexlib { namespace index {

class OfflineAttributeSegmentReaderContainer
{
private:
    using SegmentReaderMap = std::unordered_map<segmentid_t, AttributeSegmentReaderPtr>;
    using AttrSegmentReaderMap = std::unordered_map<std::string, SegmentReaderMap>;

public:
    OfflineAttributeSegmentReaderContainer(const config::IndexPartitionSchemaPtr& schema,
                                           const SegmentDirectoryBasePtr& segDir);
    ~OfflineAttributeSegmentReaderContainer();

public:
    const AttributeSegmentReaderPtr& GetAttributeSegmentReader(const std::string& attrName, segmentid_t segId);
    void Reset();

private:
    AttributeSegmentReaderPtr CreateOfflineSegmentReader(const config::AttributeConfigPtr& attrConfig,
                                                         const index_base::SegmentData& segData);

private:
    static AttributeSegmentReaderPtr NULL_READER;

private:
    autil::ThreadMutex mMapMutex;
    config::IndexPartitionSchemaPtr mSchema;
    SegmentDirectoryBasePtr mSegDir;
    AttrSegmentReaderMap mReaderMap;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(OfflineAttributeSegmentReaderContainer);
}} // namespace indexlib::index

#endif //__INDEXLIB_OFFLINE_ATTRIBUTE_SEGMENT_READER_CONTAINER_H
