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
#include "indexlib/index/normal/attribute/virtual_attribute_data_appender.h"

#include "autil/StringUtil.h"
#include "autil/ThreadPool.h"
#include "autil/WorkItem.h"
#include "indexlib/common/field_format/attribute/attribute_value_initializer.h"
#include "indexlib/common/field_format/attribute/attribute_value_initializer_creator.h"
#include "indexlib/common/field_format/attribute/default_attribute_value_initializer_creator.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index/normal/attribute/accessor/attribute_writer_factory.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/util/SimplePool.h"

using namespace std;
using namespace autil::mem_pool;
using namespace autil;
using namespace indexlib::config;
using namespace indexlib::index_base;
using namespace indexlib::common;
using namespace indexlib::file_system;
using namespace indexlib::util;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, VirtualAttributeDataAppender);

class AppendDataWorkItem : public WorkItem
{
public:
    AppendDataWorkItem(const std::function<void()>& worker) : mWorker(worker) {}

public:
    void process() override { mWorker(); }
    void destroy() override { delete this; }
    void drop() override { destroy(); }

private:
    std::function<void()> mWorker;
};

VirtualAttributeDataAppender::VirtualAttributeDataAppender(const AttributeSchemaPtr& virtualAttrSchema)
    : mVirtualAttrSchema(virtualAttrSchema)
{
}

VirtualAttributeDataAppender::~VirtualAttributeDataAppender() {}

void VirtualAttributeDataAppender::AppendData(const PartitionDataPtr& partitionData)
{
    if (!mVirtualAttrSchema) {
        return;
    }
    uint32_t threadCount = 1;
    char* threadCountEnv = secure_getenv("APPEND_VIRTUAL_ATTRIBUTE_THREAD_COUNT");
    uint32_t envThreadCount = 1;
    if (threadCountEnv != nullptr && StringUtil::strToUInt32(threadCountEnv, envThreadCount)) {
        threadCount = envThreadCount;
    }
    autil::ThreadPoolPtr threadPool;
    if (threadCount > 1) {
        threadPool.reset(new autil::ThreadPool(threadCount, mVirtualAttrSchema->GetAttributeCount()));
        threadPool->start("indexVADataAppd");
    }

    AttributeSchema::Iterator iter = mVirtualAttrSchema->Begin();
    for (; iter != mVirtualAttrSchema->End(); iter++) {
        const AttributeConfigPtr& attrConfig = *iter;
        if (threadCount > 1) {
            auto worker = [this, attrConfig, partitionData]() { this->AppendAttributeData(partitionData, attrConfig); };
            auto workItem = new AppendDataWorkItem(worker);
            if (ThreadPool::ERROR_NONE != threadPool->pushWorkItem(workItem)) {
                workItem->drop();
            }
        } else {
            AppendAttributeData(partitionData, attrConfig);
        }
    }
    if (threadPool) {
        threadPool->stop();
    }
}

void VirtualAttributeDataAppender::AppendAttributeData(const PartitionDataPtr& partitionData,
                                                       const AttributeConfigPtr& attrConfig)
{
    IE_LOG(INFO, "Begin append virtual attribute [%s] data!", attrConfig->GetAttrName().c_str());

    AttributeValueInitializerCreatorPtr creator = attrConfig->GetAttrValueInitializerCreator();
    if (!creator) {
        creator.reset(new DefaultAttributeValueInitializerCreator(attrConfig));
    }

    AttributeValueInitializerPtr initializer = creator->Create(partitionData);

    PartitionData::Iterator iter = partitionData->Begin();
    for (; iter != partitionData->End(); iter++) {
        const SegmentData& segData = *iter;
        AppendOneSegmentData(attrConfig, segData, initializer);
    }
    IE_LOG(INFO, "Finish append virtual attribute [%s] data!", attrConfig->GetAttrName().c_str());
}

void VirtualAttributeDataAppender::AppendOneSegmentData(const AttributeConfigPtr& attrConfig,
                                                        const SegmentData& segData,
                                                        const AttributeValueInitializerPtr& initializer)
{
    uint32_t docCount = segData.GetSegmentInfo()->docCount;
    if (docCount == 0) {
        return;
    }

    DirectoryPtr directory = segData.GetDirectory();
    DirectoryPtr attrDirectory = directory->GetDirectory(ATTRIBUTE_DIR_NAME, false);
    if (!attrDirectory) {
        attrDirectory = directory->MakeDirectory(ATTRIBUTE_DIR_NAME, DirectoryOption::Mem());
    }

    DirectoryPtr attrDataDirectory = attrDirectory->GetDirectory(attrConfig->GetAttrName(), false);
    if (CheckDataExist(attrConfig, attrDataDirectory)) {
        return;
    }

    // make in memory directory in local attribute dir
    attrDirectory->MakeDirectory(attrConfig->GetAttrName(), DirectoryOption::Mem());

    AttributeWriterPtr writer(
        AttributeWriterFactory::GetInstance()->CreateAttributeWriter(attrConfig, IndexPartitionOptions(), NULL));

    docid_t baseDocid = segData.GetBaseDocId();
    for (docid_t i = 0; i < (docid_t)docCount; i++) {
        StringView fieldValue;
        initializer->GetInitValue(baseDocid + i, fieldValue, &mPool);
        writer->AddField(i, fieldValue);
    }
    util::SimplePool dumpPool;
    writer->Dump(attrDirectory, &dumpPool);
}

bool VirtualAttributeDataAppender::CheckDataExist(const AttributeConfigPtr& attrConfig,
                                                  const DirectoryPtr& attrDataDirectory)
{
    if (!attrDataDirectory) {
        return false;
    }

    // check data
    if (!attrDataDirectory->IsExist(ATTRIBUTE_DATA_FILE_NAME)) {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "virtual attribute [%s] data not exist!",
                             attrConfig->GetAttrName().c_str());
    }

    if (!attrConfig->IsMultiValue() && attrConfig->GetFieldType() != ft_string) {
        return true;
    }

    // check offset
    if (!attrDataDirectory->IsExist(ATTRIBUTE_OFFSET_FILE_NAME)) {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "virtual attribute [%s] offset not exist!",
                             attrConfig->GetAttrName().c_str());
    }
    return true;
}

void VirtualAttributeDataAppender::PrepareVirtualAttrData(const config::IndexPartitionSchemaPtr& rtSchema,
                                                          const index_base::PartitionDataPtr& partData)
{
    VirtualAttributeDataAppender appender(rtSchema->GetVirtualAttributeSchema());
    appender.AppendData(partData);

    const IndexPartitionSchemaPtr& subSchema = rtSchema->GetSubIndexPartitionSchema();
    if (subSchema) {
        PartitionDataPtr subPartData = partData->GetSubPartitionData();
        VirtualAttributeDataAppender subAppender(subSchema->GetVirtualAttributeSchema());
        subAppender.AppendData(subPartData);
    }
}
}} // namespace indexlib::index
