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
#ifndef __INDEXLIB_ATTRIBUTE_FS_WRITER_PARAM_DECIDER_H
#define __INDEXLIB_ATTRIBUTE_FS_WRITER_PARAM_DECIDER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/index/common/FSWriterParamDecider.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class AttributeFSWriterParamDecider : public FSWriterParamDecider
{
public:
    AttributeFSWriterParamDecider(const config::AttributeConfigPtr& attrConfig,
                                  const config::IndexPartitionOptions& options)
        : mAttrConfig(attrConfig)
        , mOptions(options)
    {
        assert(mAttrConfig);
    }

    ~AttributeFSWriterParamDecider() {}

public:
    file_system::WriterOption MakeParam(const std::string& fileName) override;

private:
    config::AttributeConfigPtr mAttrConfig;
    config::IndexPartitionOptions mOptions;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AttributeFSWriterParamDecider);
}} // namespace indexlib::index

#endif //__INDEXLIB_ATTRIBUTE_FS_WRITER_PARAM_DECIDER_H
