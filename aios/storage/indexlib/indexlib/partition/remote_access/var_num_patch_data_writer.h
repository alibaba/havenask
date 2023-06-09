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
#ifndef __INDEXLIB_VAR_NUM_PATCH_DATA_WRITER_H
#define __INDEXLIB_VAR_NUM_PATCH_DATA_WRITER_H

#include <memory>
#include <unordered_map>

#include "indexlib/common_define.h"
#include "indexlib/index/data_structure/var_len_data_writer.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/remote_access/attribute_patch_data_writer.h"
#include "indexlib/util/SimplePool.h"

DECLARE_REFERENCE_CLASS(common, AttributeConvertor);
DECLARE_REFERENCE_CLASS(file_system, FileWriter);
DECLARE_REFERENCE_CLASS(file_system, Directory);
namespace indexlib { namespace partition {

class VarNumPatchDataWriter : public AttributePatchDataWriter
{
public:
    VarNumPatchDataWriter();
    ~VarNumPatchDataWriter();

public:
    bool Init(const config::AttributeConfigPtr& attrConfig, const config::MergeIOConfig& mergeIOConfig,
              const file_system::DirectoryPtr& directory) override;

    void AppendValue(const autil::StringView& value) override;
    void AppendNullValue() override;

    void Close() override;

    file_system::FileWriterPtr TEST_GetDataFileWriter() override { return mDataFile; }

private:
    common::AttributeConvertorPtr mAttrConvertor;
    util::SimplePool mPool;
    index::VarLenDataWriterPtr mDataWriter;
    file_system::FileWriterPtr mDataFile;
    file_system::FileWriterPtr mOffsetFile;
    file_system::FileWriterPtr mDataInfoFile;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(VarNumPatchDataWriter);
}} // namespace indexlib::partition

#endif //__INDEXLIB_VAR_NUM_PATCH_DATA_WRITER_H
