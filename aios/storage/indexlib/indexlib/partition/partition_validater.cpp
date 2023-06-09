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
#include "indexlib/partition/partition_validater.h"

#include "indexlib/index/normal/inverted_index/accessor/index_reader_factory.h"
#include "indexlib/index/normal/primarykey/legacy_primary_key_reader_interface.h"
#include "indexlib/index/primary_key/PrimaryKeyIndexReader.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/partition/index_partition_reader.h"
#include "indexlib/partition/offline_partition.h"

using namespace std;

namespace indexlib::partition {
AUTIL_LOG_SETUP(indexlib.partition, PartitionValidater);

PartitionValidater::PartitionValidater(const config::IndexPartitionOptions& options) : mOptions(options)
{
    mOptions.SetIsOnline(false);
    mOptions.GetOfflineConfig().readerConfig.loadIndex = true;
    mOptions.GetOfflineConfig().enableRecoverIndex = false;
}

PartitionValidater::~PartitionValidater() {}

bool PartitionValidater::Init(const std::string& indexPath, versionid_t versionId)
{
    mSchema = index_base::SchemaAdapter::LoadSchemaByVersionId(indexPath, versionId);
    assert(mSchema);
    if (mSchema->GetTableType() == tt_customized) { // not validate customized table
        return true;
    }

    mPartition.reset(new OfflinePartition);
    auto ret = mPartition->Open(indexPath, "", mSchema, mOptions);
    if (ret != IndexPartition::OS_OK) {
        AUTIL_LOG(ERROR, "Open latest version fail, errorCode [%d]", (int)ret);
        return false;
    }
    mSchema = mPartition->GetSchema();
    return true;
}

// for now on, only check pk reader
// TODO: check offline reader block cache load index
// current not ready for kv & kkv separate_chain hash table & normal index with fp8/fp16 compress attribute
void PartitionValidater::Check()
{
    assert(mSchema);
    if (mSchema->GetTableType() != tt_index) {
        return;
    }

    auto indexSchema = mSchema->GetIndexSchema();
    if (!indexSchema) {
        return;
    }

    auto pkIndexConfig = indexSchema->GetPrimaryKeyIndexConfig();
    if (!pkIndexConfig) {
        return;
    }

    auto pkIndexType = pkIndexConfig->GetInvertedIndexType();
    if (pkIndexType != it_primarykey64 && pkIndexType != it_primarykey128) {
        return;
    }

    assert(mPartition);
    std::shared_ptr<index::PrimaryKeyIndexReader> pkReader(
        index::IndexReaderFactory::CreatePrimaryKeyIndexReader(pkIndexType));
    assert(pkReader);
    const auto& legacyPkReader = std::dynamic_pointer_cast<index::LegacyPrimaryKeyReaderInterface>(pkReader);
    legacyPkReader->OpenWithoutPKAttribute(pkIndexConfig, mPartition->GetPartitionData());
    if (!pkReader->CheckDuplication()) {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "get pk reader duplication fail.");
    }
}

} // namespace indexlib::partition
