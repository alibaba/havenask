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
#include "indexlib/partition/modifier/partition_modifier_task_item.h"

#include <memory>

#include "indexlib/config/index_partition_schema.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/index/normal/attribute/accessor/attribute_modifier.h"
#include "indexlib/index_define.h"
#include "indexlib/partition/modifier/patch_modifier.h"

using namespace autil::mem_pool;
using namespace indexlib::config;
using namespace indexlib::document;
using namespace indexlib::index;
using namespace indexlib::file_system;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, PartitionModifierDumpTaskItem);

void PartitionModifierDumpTaskItem::Dump(const config::IndexPartitionSchemaPtr& schema,
                                         const file_system::DirectoryPtr& directory, segmentid_t srcSegmentId,
                                         uint32_t threadCnt)
{
    if (taskType == PartitionModifierDumpTaskType::InplaceModifierTask) {
        if (packAttrUpdataBitmapItems.size() > 0) {
            DirectoryPtr attrDir = directory->MakeDirectory(ATTRIBUTE_DIR_NAME);
            AttributeModifier::DumpPackAttributeUpdateInfo(attrDir, schema, packAttrUpdataBitmapItems[0]);
        }
    } else if (taskType == PartitionModifierDumpTaskType::SubDocModifierTask) {
        // sub modifier must before main modifier
        if (isOnline) {
            if (packAttrUpdataBitmapItems.size() == 2) {
                const IndexPartitionSchemaPtr& subSchema = schema->GetSubIndexPartitionSchema();
                if (subSchema == nullptr) {
                    INDEXLIB_FATAL_ERROR(Runtime, "cannot create SubDocModifier without sub schema");
                }
                DirectoryPtr subDirectory = directory->GetDirectory(SUB_SEGMENT_DIR_NAME, true);
                assert(subDirectory);
                DirectoryPtr subAttrDir = subDirectory->MakeDirectory(ATTRIBUTE_DIR_NAME);
                AttributeModifier::DumpPackAttributeUpdateInfo(subAttrDir, subSchema, packAttrUpdataBitmapItems[0]);
                DirectoryPtr mainAttrDir = directory->MakeDirectory(ATTRIBUTE_DIR_NAME);
                AttributeModifier::DumpPackAttributeUpdateInfo(mainAttrDir, schema, packAttrUpdataBitmapItems[1]);
            }
        } else {
            assert(patchModifiers.size() == 2);
            DirectoryPtr subDirectory = directory->GetDirectory(SUB_SEGMENT_DIR_NAME, true);
            assert(subDirectory);
            patchModifiers[0]->SetDumpThreadNum(threadCnt);
            patchModifiers[0]->Dump(subDirectory, srcSegmentId);
            patchModifiers[1]->SetDumpThreadNum(threadCnt);
            patchModifiers[1]->Dump(directory, srcSegmentId);
        }
    } else if (taskType == PartitionModifierDumpTaskType::PatchModifierTask) {
        assert(patchModifiers.size() == 1);
        patchModifiers[0]->SetDumpThreadNum(threadCnt);
        patchModifiers[0]->Dump(directory, srcSegmentId);
    } else {
        assert(false);
    }
}
}} // namespace indexlib::partition
