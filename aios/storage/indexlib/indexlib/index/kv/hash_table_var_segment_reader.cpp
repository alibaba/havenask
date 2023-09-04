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
#include "indexlib/index/kv/hash_table_var_segment_reader.h"

using namespace std;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, HashTableVarSegmentReader);

void HashTableVarSegmentReader::Open(const config::KVIndexConfigPtr& kvIndexConfig,
                                     const index_base::SegmentData& segmentData)
{
    if (segmentData.GetSegmentInfo()->HasMultiSharding()) {
        INDEXLIB_FATAL_ERROR(UnSupported, "kv can not open segment [%d] with multi sharding data",
                             segmentData.GetSegmentId());
    }
    mSegmentId = segmentData.GetSegmentId();
    mIsMultiRegion = kvIndexConfig->GetRegionCount() > 1;
    if (!mIsMultiRegion) {
        mFixValueLen = kvIndexConfig->GetValueConfig()->GetFixedLength();
        mHasFixValueLen = (mFixValueLen > 0);
    }
    file_system::DirectoryPtr directory = segmentData.GetDirectory();
    file_system::DirectoryPtr indexDir = directory->GetDirectory(INDEX_DIR_NAME, true);
    file_system::DirectoryPtr kvDir = indexDir->GetDirectory(kvIndexConfig->GetIndexName(), true);
    bool isOnlineSegment = index_base::RealtimeSegmentDirectory::IsRtSegmentId(mSegmentId);
    if (!mOffsetReader.Open(kvDir, kvIndexConfig, isOnlineSegment, segmentData.IsBuildingSegment())) {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "kv open key file fail for segment [%d]!", segmentData.GetSegmentId());
    }
    if (!OpenValue(kvDir, kvIndexConfig, segmentData)) {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "kv open value file fail for segment [%d]!", segmentData.GetSegmentId());
    }
    if (!isOnlineSegment && mValueBaseAddr) {
        // online segment: building segment has base addr, built segment may ondisk
        // offline segment: if segment all delete doc, valueBaseAdress==NULL
        mCodegenHasBaseAddr = true;
    }
}

bool HashTableVarSegmentReader::OpenValue(const file_system::DirectoryPtr& kvDir,
                                          const config::KVIndexConfigPtr& kvIndexConfig,
                                          const index_base::SegmentData& segmentData)
{
    bool useMmap = UseSwapMMapFile(kvDir, kvIndexConfig, segmentData);
    std::string fileName = GetValueFileName(segmentData);
    mValueFileReader = CreateValueFileReader(kvDir, kvIndexConfig, fileName, useMmap);
    if (!mValueFileReader) {
        IE_LOG(ERROR, "create value file reader failed in dir[%s]", kvDir->DebugString().c_str());
        return false;
    }
    mValueBaseAddr = (char*)mValueFileReader->GetBaseAddress();
    if (mValueBaseAddr) {
        mUseBaseAddr = true;
    }
    return true;
}

std::string HashTableVarSegmentReader::GetValueFileName(const index_base::SegmentData& segmentData) const
{
    std::string ret = std::string(KV_VALUE_FILE_NAME);
    if (segmentData.IsBuildingSegment()) {
        ret = ret + ".tmp";
    }
    return ret;
}

bool HashTableVarSegmentReader::UseSwapMMapFile(const file_system::DirectoryPtr& dir,
                                                const config::KVIndexConfigPtr& kvIndexConfig,
                                                const index_base::SegmentData& segmentData) const
{
    return segmentData.IsBuildingSegment() && kvIndexConfig->GetUseSwapMmapFile();
}

bool HashTableVarSegmentReader::doCollectAllCode()
{
    INIT_CODEGEN_INFO(HashTableVarSegmentReader, true);
    COLLECT_CONST_MEM_VAR(mIsMultiRegion);
    COLLECT_CONST_MEM_VAR(mHasFixValueLen);
    if (mCodegenHasBaseAddr) {
        COLLECT_CONST_MEM_VAR(mUseBaseAddr);
    }
    mOffsetReader.collectAllCode();
    COLLECT_TYPE_REDEFINE(OffsetReaderType, mOffsetReader.getTargetClassName());
    combineCodegenInfos(&mOffsetReader);
    return true;
}

void HashTableVarSegmentReader::TEST_collectCodegenResult(CodegenCheckers& checkers, std::string id)
{
    codegen::CodegenCheckerPtr checker(new codegen::CodegenChecker);
    COLLECT_CONST_MEM(checker, mIsMultiRegion);
    COLLECT_CONST_MEM(checker, mHasFixValueLen);
    COLLECT_CONST_MEM(checker, mUseBaseAddr);
    COLLECT_TYPE_DEFINE(checker, OffsetReaderType);
    checkers[string("HashTableVarSegmentReader") + id] = checker;
    mOffsetReader.TEST_collectCodegenResult(checkers, id);
}
}} // namespace indexlib::index
