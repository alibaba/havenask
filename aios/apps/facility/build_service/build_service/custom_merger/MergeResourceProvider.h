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
#ifndef ISEARCH_BS_MERGERESOURCEPROVIDER_H
#define ISEARCH_BS_MERGERESOURCEPROVIDER_H

#include "build_service/common_define.h"
#include "build_service/custom_merger/TaskItemDispatcher.h"
#include "build_service/util/Log.h"
#include "indexlib/partition/remote_access/partition_resource_provider.h"

namespace build_service { namespace custom_merger {

class MergeResourceProvider
{
public:
    MergeResourceProvider();
    virtual ~MergeResourceProvider();

private:
    MergeResourceProvider(const MergeResourceProvider&);
    MergeResourceProvider& operator=(const MergeResourceProvider&);

public:
    bool init(const std::string& indexPath, const indexlib::config::IndexPartitionOptions& options,
              const indexlib::config::IndexPartitionSchemaPtr& newSchema, const std::string& pluginPath,
              int32_t targetVersionId, int32_t checkpointVersionId);

    typedef TaskItemDispatcher::SegmentInfo SegmentInfo;
    indexlib::index_base::Version getVersion() { return _indexProvider->GetVersion(); }
    virtual void getIndexSegmentInfos(std::vector<SegmentInfo>& segmentInfos);

    virtual indexlib::config::IndexPartitionSchemaPtr getNewSchema() { return _newSchema; }

    virtual indexlib::config::IndexPartitionSchemaPtr getOldSchema()
    {
        if (!_newSchema->HasModifyOperations()) {
            return _indexProvider->GetSchema();
        }
        indexlib::config::IndexPartitionSchemaPtr oldSchema;
        uint32_t operationCount = _newSchema->GetModifyOperationCount();
        if (operationCount == 1) {
            oldSchema.reset(_newSchema->CreateSchemaWithoutModifyOperations());
        } else {
            oldSchema.reset(_newSchema->CreateSchemaForTargetModifyOperation(operationCount - 1));
        }
        return oldSchema;
    }

    virtual indexlib::partition::PartitionResourceProviderPtr getPartitionResourceProvider() { return _indexProvider; }

protected:
    indexlib::config::IndexPartitionSchemaPtr _newSchema;
    indexlib::partition::PartitionResourceProviderPtr _indexProvider;
    int32_t _checkpointVersionId;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(MergeResourceProvider);

}} // namespace build_service::custom_merger

#endif // ISEARCH_BS_MERGERESOURCEPROVIDER_H
