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
#include "indexlib/index/calculator/partition_size_calculator.h"

#include "indexlib/config/index_partition_schema.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/index/calculator/segment_lock_size_calculator.h"
#include "indexlib/index/normal/framework/legacy_index_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/index_reader_factory.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/patch/partition_patch_index_accessor.h"
#include "indexlib/index_define.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/counter/MultiCounter.h"

using namespace std;

using namespace indexlib::config;
using namespace indexlib::file_system;
using namespace indexlib::util;
using namespace indexlib::index_base;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, PartitionSizeCalculator);

PartitionSizeCalculator::PartitionSizeCalculator(const DirectoryPtr& directory, const IndexPartitionSchemaPtr& schema,
                                                 bool throwExceptionIfNotExist,
                                                 const plugin::PluginManagerPtr& pluginManager)
    : mDirectory(directory)
    , mSchema(schema)
    , mThrowExceptionIfNotExist(throwExceptionIfNotExist)
    , mPluginManager(pluginManager)
{
}

PartitionSizeCalculator::~PartitionSizeCalculator() {}

size_t PartitionSizeCalculator::CalculateDiffVersionLockSizeWithoutPatch(const Version& version,
                                                                         const Version& lastLoadVersion,
                                                                         const PartitionDataPtr& partitionData,
                                                                         const MultiCounterPtr& counter)
{
    assert(mSchema);
    if (!mDirectory) {
        return 0;
    }

    SegmentLockSizeCalculatorPtr segmentCalculator = CreateSegmentCalculator(mSchema);
    IndexPartitionSchemaPtr subSchema = mSchema->GetSubIndexPartitionSchema();
    SegmentLockSizeCalculatorPtr subSegmentCalculator = CreateSegmentCalculator(subSchema);

    vector<string> segPathVec;
    GetSegmentPathVec(version, segPathVec);
    vector<string> lastVersionSegPathVec;
    GetSegmentPathVec(lastLoadVersion, lastVersionSegPathVec);
    vector<string> diffSegPathVec;
    set_difference(segPathVec.begin(), segPathVec.end(), lastVersionSegPathVec.begin(), lastVersionSegPathVec.end(),
                   inserter(diffSegPathVec, diffSegPathVec.begin()));
    map<string, pair<string, string>> temperatureChangeSegs;
    AddChangedTemperatueSegment(version, lastLoadVersion, temperatureChangeSegs);

    // sort diff segments:
    // dimension 1: segmentid from big to small
    // dimension 2: patch schemaId from big to small
    sort(diffSegPathVec.begin(), diffSegPathVec.end(), [](const string& lft, const string& rht) -> bool {
        segmentid_t lftSegId = Version::GetSegmentIdByDirName(PathUtil::GetFileName(lft));
        segmentid_t rhtSegId = Version::GetSegmentIdByDirName(PathUtil::GetFileName(rht));
        if (lftSegId == rhtSegId) {
            schemaid_t lftSchemaId = 0;
            PartitionPatchIndexAccessor::ExtractSchemaIdFromPatchRootDir(PathUtil::GetParentDirName(lft), lftSchemaId);
            schemaid_t rhtSchemaId = 0;
            PartitionPatchIndexAccessor::ExtractSchemaIdFromPatchRootDir(PathUtil::GetParentDirName(rht), rhtSchemaId);
            return lftSchemaId > rhtSchemaId;
        }
        return lftSegId > rhtSegId;
    });

    size_t totalSize = 0;
    for (size_t i = 0; i < diffSegPathVec.size(); ++i) {
        const string& segDirName = diffSegPathVec[i];
        DirectoryPtr segDir = mDirectory->GetDirectory(segDirName, mThrowExceptionIfNotExist);
        if (segDir) {
            auto segCounter = counter ? counter->CreateMultiCounter(segDirName) : counter;
            totalSize += segmentCalculator->CalculateSize(segDir, segCounter);
        } else {
            IE_LOG(WARN, "segDir [%s] not exist in [%s]", segDirName.c_str(), mDirectory->DebugString().c_str());
        }

        if (subSegmentCalculator) {
            DirectoryPtr subSegDir = segDir->GetDirectory(SUB_SEGMENT_DIR_NAME, mThrowExceptionIfNotExist);
            if (subSegDir) {
                auto subSegCounter =
                    counter ? counter->CreateMultiCounter(segDirName + "/" + SUB_SEGMENT_DIR_NAME) : counter;
                totalSize += subSegmentCalculator->CalculateSize(subSegDir, subSegCounter);
            } else {
                IE_LOG(WARN, "sub_segment not exist in [%s]", segDir->DebugString().c_str());
            }
        }
    }

    auto iter = temperatureChangeSegs.begin();
    while (iter != temperatureChangeSegs.end()) {
        const string& segDirName = iter->first;
        DirectoryPtr segDir = mDirectory->GetDirectory(segDirName, mThrowExceptionIfNotExist);
        if (segDir) {
            totalSize += segmentCalculator->CalculateDiffSize(segDir, iter->second.first, iter->second.second);
        }
        if (subSegmentCalculator) {
            DirectoryPtr subSegDir = segDir->GetDirectory(SUB_SEGMENT_DIR_NAME, mThrowExceptionIfNotExist);
            if (subSegDir) {
                totalSize +=
                    subSegmentCalculator->CalculateDiffSize(subSegDir, iter->second.first, iter->second.second);
            }
        }
        iter++;
    }

    if (mSchema->GetTableType() == tt_index) {
        if (mSchema->GetIndexSchema()->HasPrimaryKeyIndex()) {
            auto pkCounter = counter ? counter->CreateMultiCounter("pk") : counter;
            totalSize += CalculatePkSize(partitionData, lastLoadVersion, pkCounter);
        }
    }
    return totalSize;
}

void PartitionSizeCalculator::AddChangedTemperatueSegment(const Version& version, const Version& lastLoadVersion,
                                                          map<string, pair<string, string>>& diffSegPath)
{
    auto oldSegmentTemperatures = lastLoadVersion.GetSegTemperatureMetas();
    if (oldSegmentTemperatures.empty()) {
        return;
    }
    for (const auto& oldSegTemperature : oldSegmentTemperatures) {
        SegmentTemperatureMeta newMeta;
        if (version.GetSegmentTemperatureMeta(oldSegTemperature.segmentId, newMeta)) {
            if (newMeta.segTemperature != oldSegTemperature.segTemperature) {
                diffSegPath[version.GetSegmentDirName(oldSegTemperature.segmentId)] =
                    make_pair(oldSegTemperature.segTemperature, newMeta.segTemperature);
                IE_LOG(INFO, "segment [%d] temperature has change from [%s] to [%s], need recalculator size",
                       oldSegTemperature.segmentId, oldSegTemperature.segTemperature.c_str(),
                       newMeta.segTemperature.c_str());
            }
        }
    }
}

size_t PartitionSizeCalculator::CalculatePkSize(const PartitionDataPtr& partitionData, const Version& lastLoadVersion,
                                                const MultiCounterPtr& counter)
{
    size_t totalSize = 0;
    size_t mainSize = CalculatePkReaderLoadSize(mSchema, partitionData, lastLoadVersion);
    totalSize += mainSize;
    if (counter) {
        counter->CreateStateCounter("main")->Set(mainSize);
    }
    const IndexPartitionSchemaPtr& subSchema = mSchema->GetSubIndexPartitionSchema();
    if (subSchema) {
        size_t subSize = CalculatePkReaderLoadSize(subSchema, partitionData->GetSubPartitionData(), lastLoadVersion);
        totalSize += subSize;
        if (counter) {
            counter->CreateStateCounter("sub")->Set(subSize);
        }
    }
    return totalSize;
}

size_t PartitionSizeCalculator::CalculatePkReaderLoadSize(const config::IndexPartitionSchemaPtr& schema,
                                                          const index_base::PartitionDataPtr& partitionData,
                                                          const Version& lastLoadVersion)
{
    IndexConfigPtr pkIndexConfig = schema->GetIndexSchema()->GetPrimaryKeyIndexConfig();
    if (!pkIndexConfig || pkIndexConfig->GetInvertedIndexType() == it_kkv ||
        pkIndexConfig->GetInvertedIndexType() == it_kv) {
        return 0;
    }
    std::shared_ptr<InvertedIndexReader> pkIndexReader(
        IndexReaderFactory::CreateIndexReader(pkIndexConfig->GetInvertedIndexType(), /*indexMetrics*/ nullptr));
    const auto& legacyIndexReader = std::dynamic_pointer_cast<index::LegacyIndexReaderInterface>(pkIndexReader);
    assert(legacyIndexReader);
    return legacyIndexReader->EstimateLoadSize(partitionData, pkIndexConfig, lastLoadVersion);
}

SegmentLockSizeCalculatorPtr PartitionSizeCalculator::CreateSegmentCalculator(const IndexPartitionSchemaPtr& schema)
{
    if (!schema) {
        return SegmentLockSizeCalculatorPtr();
    }
    return SegmentLockSizeCalculatorPtr(new SegmentLockSizeCalculator(schema, mPluginManager, true));
}

void PartitionSizeCalculator::GetSegmentPathVec(const Version& version, vector<string>& segPathVec)
{
    assert(segPathVec.empty());
    for (size_t i = 0; i < version.GetSegmentCount(); ++i) {
        string segDirName = version.GetSegmentDirName(version[i]);
        segPathVec.push_back(segDirName);
    }

    if (version.GetVersionId() != INVALID_VERSIONID && version.GetSchemaVersionId() != DEFAULT_SCHEMAID) {
        PartitionPatchMeta patchMeta;
        patchMeta.Load(mDirectory, version.GetVersionId());
        PartitionPatchMeta::Iterator metaIter = patchMeta.CreateIterator();
        while (metaIter.HasNext()) {
            SchemaPatchInfoPtr schemaInfo = metaIter.Next();
            assert(schemaInfo);
            SchemaPatchInfo::Iterator sIter = schemaInfo->Begin();
            for (; sIter != schemaInfo->End(); sIter++) {
                const SchemaPatchInfo::PatchSegmentMeta& segMeta = *sIter;
                if (!version.HasSegment(segMeta.GetSegmentId())) {
                    continue;
                }
                string patchSegPath =
                    PathUtil::JoinPath(PartitionPatchIndexAccessor::GetPatchRootDirName(schemaInfo->GetSchemaId()),
                                       version.GetSegmentDirName(segMeta.GetSegmentId()));
                segPathVec.push_back(patchSegPath);
            }
        }
    }
    sort(segPathVec.begin(), segPathVec.end());
}
}} // namespace indexlib::index
