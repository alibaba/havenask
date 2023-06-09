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
#ifndef __INDEXLIB_VIRTUAL_ATTRIBUTE_DATA_APPENDER_H
#define __INDEXLIB_VIRTUAL_ATTRIBUTE_DATA_APPENDER_H

#include <memory>

#include "autil/mem_pool/Pool.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(config, AttributeSchema);
DECLARE_REFERENCE_CLASS(config, AttributeConfig);
DECLARE_REFERENCE_CLASS(common, AttributeValueInitializer);
DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(index_base, SegmentData);
DECLARE_REFERENCE_CLASS(file_system, Directory);

namespace indexlib { namespace index {

class VirtualAttributeDataAppender
{
public:
    VirtualAttributeDataAppender(const config::AttributeSchemaPtr& virtualAttrSchema);
    ~VirtualAttributeDataAppender();

public:
    void AppendData(const index_base::PartitionDataPtr& partitionData);

public:
    static void PrepareVirtualAttrData(const config::IndexPartitionSchemaPtr& rtSchema,
                                       const index_base::PartitionDataPtr& partData);

private:
    void AppendAttributeData(const index_base::PartitionDataPtr& partitionData,
                             const config::AttributeConfigPtr& attrConfig);

    void AppendOneSegmentData(const config::AttributeConfigPtr& attrConfig, const index_base::SegmentData& segData,
                              const common::AttributeValueInitializerPtr& initializer);

    bool CheckDataExist(const config::AttributeConfigPtr& attrConfig,
                        const file_system::DirectoryPtr& attrDataDirectory);

private:
    config::AttributeSchemaPtr mVirtualAttrSchema;
    autil::mem_pool::Pool mPool;

private:
    friend class VirtualAttributeDataAppenderTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(VirtualAttributeDataAppender);
}} // namespace indexlib::index

#endif //__INDEXLIB_VIRTUAL_ATTRIBUTE_DATA_APPENDER_H
