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
#ifndef __INDEXLIB_ATTRIBUTE_PATCH_DATA_WRITER_H
#define __INDEXLIB_ATTRIBUTE_PATCH_DATA_WRITER_H

#include <memory>

#include "autil/ConstString.h"
#include "indexlib/common_define.h"
#include "indexlib/config/merge_io_config.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(config, AttributeConfig);
DECLARE_REFERENCE_CLASS(file_system, FileWriter);
DECLARE_REFERENCE_CLASS(file_system, Directory);

namespace indexlib { namespace partition {

class AttributePatchDataWriter
{
public:
    AttributePatchDataWriter() {}
    virtual ~AttributePatchDataWriter() {}

public:
    virtual bool Init(const config::AttributeConfigPtr& attrConfig, const config::MergeIOConfig& mergeIOConfig,
                      const file_system::DirectoryPtr& directory) = 0;

    virtual void AppendValue(const autil::StringView& value) = 0;
    virtual void AppendNullValue() = 0;

    virtual void Close() = 0;
    virtual file_system::FileWriterPtr TEST_GetDataFileWriter() = 0;

protected:
    file_system::FileWriter* CreateBufferedFileWriter(size_t bufferSize, bool asyncWrite);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AttributePatchDataWriter);
}} // namespace indexlib::partition

#endif //__INDEXLIB_ATTRIBUTE_PATCH_DATA_WRITER_H
