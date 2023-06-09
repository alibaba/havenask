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
#include "indexlib/partition/segment/compress_ratio_calculator.h"

#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/kkv_index_config.h"
#include "indexlib/config/kv_index_config.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/file/CompressFileInfo.h"
#include "indexlib/index/kkv/kkv_define.h"
#include "indexlib/index/kv/kv_define.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/index_define.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace indexlib::config;
using namespace indexlib::util;
using namespace indexlib::index;
using namespace indexlib::file_system;
using namespace indexlib::index_base;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, CompressRatioCalculator);

CompressRatioCalculator::CompressRatioCalculator() {}

CompressRatioCalculator::~CompressRatioCalculator() {}

void CompressRatioCalculator::Init(const PartitionDataPtr& partData, const IndexPartitionSchemaPtr& schema)
{
    assert(partData);
    assert(schema);
    if (schema->GetTableType() == tt_kkv) {
        KKVIndexConfigPtr kkvConfig = CreateDataKKVIndexConfig(schema);
        assert(kkvConfig);
        InitKKVCompressRatio(kkvConfig, partData);
    }

    if (schema->GetTableType() == tt_kv) {
        KVIndexConfigPtr kvConfig = CreateDataKVIndexConfig(schema);
        assert(kvConfig);
        InitKVCompressRatio(kvConfig, partData);
    }
}

void CompressRatioCalculator::InitKKVCompressRatio(const KKVIndexConfigPtr& kkvConfig, const PartitionDataPtr& partData)
{
    assert(kkvConfig);
    const config::KKVIndexPreference& kkvIndexPreference = kkvConfig->GetIndexPreference();
    const config::KKVIndexPreference::SuffixKeyParam& skeyParam = kkvIndexPreference.GetSkeyParam();
    const config::KKVIndexPreference::ValueParam& valueParam = kkvIndexPreference.GetValueParam();
    if (!skeyParam.EnableFileCompress() && !valueParam.EnableFileCompress()) {
        return;
    }

    index_base::SegmentDataVector segmentDataVec;
    GetSegmentDatas(partData, segmentDataVec);
    if (skeyParam.EnableFileCompress()) {
        string indexDirPath = PathUtil::JoinPath(INDEX_DIR_NAME, kkvConfig->GetIndexName());
        string filePath = PathUtil::JoinPath(indexDirPath, SUFFIX_KEY_FILE_NAME);
        mRatioMap[SUFFIX_KEY_FILE_NAME] = GetRatio(segmentDataVec, filePath);
    }

    if (valueParam.EnableFileCompress()) {
        string indexDirPath = PathUtil::JoinPath(INDEX_DIR_NAME, kkvConfig->GetIndexName());
        string filePath = PathUtil::JoinPath(indexDirPath, KKV_VALUE_FILE_NAME);
        mRatioMap[KKV_VALUE_FILE_NAME] = GetRatio(segmentDataVec, filePath);
    }
}

void CompressRatioCalculator::InitKVCompressRatio(const KVIndexConfigPtr& kvConfig, const PartitionDataPtr& partData)
{
    assert(kvConfig);
    const config::KVIndexPreference& kvIndexPreference = kvConfig->GetIndexPreference();
    const config::KVIndexPreference::ValueParam& valueParam = kvIndexPreference.GetValueParam();
    if (!valueParam.EnableFileCompress()) {
        return;
    }

    index_base::SegmentDataVector segmentDataVec;
    GetSegmentDatas(partData, segmentDataVec);
    if (valueParam.EnableFileCompress()) {
        string indexDirPath = PathUtil::JoinPath(INDEX_DIR_NAME, kvConfig->GetIndexName());
        string filePath = PathUtil::JoinPath(indexDirPath, KV_VALUE_FILE_NAME);
        mRatioMap[KV_VALUE_FILE_NAME] = GetRatio(segmentDataVec, filePath);
    }
}

double CompressRatioCalculator::GetCompressRatio(const string& fileName) const
{
    RatioMap::const_iterator iter = mRatioMap.find(fileName);
    return iter != mRatioMap.end() ? iter->second : DEFAULT_COMPRESS_RATIO;
}

double CompressRatioCalculator::GetRatio(const SegmentDataVector& segDataVec, const string& fileName)
{
    SegmentDataVector::const_reverse_iterator rIter = segDataVec.rbegin();
    for (; rIter != segDataVec.rend(); rIter++) {
        const SegmentData& segData = *rIter;
        file_system::DirectoryPtr segDirectory = segData.GetDirectory();
        ;
        if (!segDirectory) {
            continue;
        }

        CompressFileInfoPtr compressFileInfo = segDirectory->GetCompressFileInfo(fileName);
        if (compressFileInfo && compressFileInfo->deCompressFileLen > 0) {
            return (double)compressFileInfo->compressFileLen / compressFileInfo->deCompressFileLen;
        }
    }
    return DEFAULT_COMPRESS_RATIO;
}

void CompressRatioCalculator::GetSegmentDatas(const PartitionDataPtr& partData, SegmentDataVector& segmentDataVec)
{
    for (index_base::PartitionData::Iterator iter = partData->Begin(); iter != partData->End(); ++iter) {
        if (!(*iter).GetSegmentInfo()->HasMultiSharding()) {
            segmentDataVec.push_back(*iter);
            continue;
        }

        uint32_t segmentColumnCount = (*iter).GetSegmentInfo()->shardCount;
        for (size_t inSegmentIdx = 0; inSegmentIdx < segmentColumnCount; ++inSegmentIdx) {
            const index_base::SegmentData& shardingSegmentData = (*iter).CreateShardingSegmentData(inSegmentIdx);
            segmentDataVec.push_back(shardingSegmentData);
        }
    }
}
}} // namespace indexlib::partition
