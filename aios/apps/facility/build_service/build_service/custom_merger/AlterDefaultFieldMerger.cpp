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
#include "build_service/custom_merger/AlterDefaultFieldMerger.h"

#include "autil/StringUtil.h"
#include "build_service/custom_merger/MergeResourceProvider.h"
#include "fslib/util/FileUtil.h"
#include "indexlib/config/schema_differ.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/normal/attribute/accessor/attribute_data_iterator.h"

using namespace std;
using namespace autil;
using namespace build_service::util;
using namespace indexlib::config;
using namespace indexlib::index;
using namespace indexlib::partition;
using namespace indexlib::index_base;
using namespace indexlib::file_system;

namespace build_service { namespace custom_merger {
BS_LOG_SETUP(custom_merger, AlterDefaultFieldMerger);

const std::string AlterDefaultFieldMerger::MERGER_NAME = "AlterDefaultFieldMerger";

AlterDefaultFieldMerger::AlterDefaultFieldMerger(uint32_t backupId, const string& epochId)
    : CustomMergerImpl::CustomMergerImpl(backupId, epochId)
{
}

AlterDefaultFieldMerger::~AlterDefaultFieldMerger() {}

bool AlterDefaultFieldMerger::estimateCost(std::vector<CustomMergerTaskItem>& taskItems)
{
    IndexPartitionSchemaPtr newSchema = _param.resourceProvider->getNewSchema();
    IndexPartitionSchemaPtr oldSchema = _param.resourceProvider->getOldSchema();
    vector<AttributeConfigPtr> alterAttributes;
    vector<IndexConfigPtr> alterIndexes;
    string errorMsg;
    SchemaDiffer::CalculateAlterFields(oldSchema, newSchema, alterAttributes, alterIndexes, errorMsg);
    for (size_t i = 0; i < alterAttributes.size(); i++) {
        CustomMergerTaskItem item;
        assert(alterAttributes[i]);
        string attrName = alterAttributes[i]->GetAttrName();
        item.fields.emplace_back(attrName, CustomMergerField::ATTRIBUTE);
        item.cost = 1.0;
        taskItems.push_back(item);
    }

    for (size_t i = 0; i < alterIndexes.size(); i++) {
        CustomMergerTaskItem item;
        CustomMergerField field(alterIndexes[i]->GetIndexName(), CustomMergerField::INDEX);
        item.fields.push_back(field);
        item.cost = 0.0;
        taskItems.push_back(item);
    }
    return true;
}

bool AlterDefaultFieldMerger::doMergeTask(const CustomMergePlan::TaskItem& item,
                                          const indexlib::file_system::DirectoryPtr& dir)
{
    // only for test
    {
        auto it = _param.parameters.find("sleep_time");
        if (it != _param.parameters.end()) {
            int64_t sleepTime = 0;
            if (StringUtil::fromString(it->second, sleepTime)) {
                BS_LOG(INFO, "alter field sleep time %ld", sleepTime);
                sleep(sleepTime);
            }
        }
    }
    MergeResourceProviderPtr provider = _param.resourceProvider;
    PartitionResourceProviderPtr partitionProvider = provider->getPartitionResourceProvider();
    PartitionIteratorPtr partDataIter = partitionProvider->CreatePartitionIterator();
    PartitionPatcherPtr patcher = partitionProvider->CreatePartitionPatcher(provider->getNewSchema(), dir);
    for (auto field : item.taskItem.fields) {
        for (auto segmentId : item.segments) {
            if (field.fieldType == CustomMergerField::INDEX) {
                IndexDataPatcherPtr indexPatcher = patcher->CreateSingleIndexPatcher(field.fieldName, segmentId);
                if (!indexPatcher) {
                    BS_LOG(ERROR, "create index [%s] patcher failed", field.fieldName.c_str());
                    return false;
                }
                // when close patcher, auto patch default index
                indexPatcher->Close();
            } else if (field.fieldType == CustomMergerField::ATTRIBUTE) {
                AttributeDataPatcherPtr attrPatcher = patcher->CreateSingleAttributePatcher(field.fieldName, segmentId);
                if (!attrPatcher) {
                    BS_LOG(ERROR, "create attribute [%s] patcher failed", field.fieldName.c_str());
                    return false;
                }
                FillAttributeValue(partDataIter, field.fieldName, segmentId, attrPatcher);
                // when release patcher, auto patch default attribute
                attrPatcher->Close();
            } else {
                BS_LOG(ERROR, "field [%s] has unkown type [%d]", field.fieldName.c_str(), field.fieldType);
                return false;
            }
        }
    }
    partitionProvider->Sync();
    return true;
}

void AlterDefaultFieldMerger::FillAttributeValue(const PartitionIteratorPtr& partIter, const string& attrName,
                                                 segmentid_t segmentId, const AttributeDataPatcherPtr& attrPatcher)
{
    // do nothing
}

}} // namespace build_service::custom_merger
