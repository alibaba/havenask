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
#include "indexlib/index/kv/hash_table_compress_var_segment_reader.h"

using namespace std;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, HashTableCompressVarSegmentReader);

void HashTableCompressVarSegmentReader::Open(const config::KVIndexConfigPtr& kvIndexConfig,
                                             const index_base::SegmentData& segmentData)
{
    if (segmentData.GetSegmentInfo()->HasMultiSharding()) {
        INDEXLIB_FATAL_ERROR(UnSupported, "kv can not open segment [%d] with multi sharding data",
                             segmentData.GetSegmentId());
    }

    file_system::DirectoryPtr directory = segmentData.GetDirectory();
    file_system::DirectoryPtr indexDir = directory->GetDirectory(INDEX_DIR_NAME, true);
    file_system::DirectoryPtr kvDir = indexDir->GetDirectory(kvIndexConfig->GetIndexName(), true);
    bool isOnlineSegment = index_base::RealtimeSegmentDirectory::IsRtSegmentId(segmentData.GetSegmentId());
    if (!mOffsetReader.Open(kvDir, kvIndexConfig, isOnlineSegment, segmentData.IsBuildingSegment())) {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "open kv key fail for segment [%d]!", segmentData.GetSegmentId());
    }

    if (!OpenValue(kvDir, kvIndexConfig)) {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "open kv value fail for segment [%d]!", segmentData.GetSegmentId());
    }
}

bool HashTableCompressVarSegmentReader::OpenValue(const file_system::DirectoryPtr& kvDir,
                                                  const config::KVIndexConfigPtr& kvIndexConfig)
{
    const config::KVIndexPreference::ValueParam& valueParam = kvIndexConfig->GetIndexPreference().GetValueParam();
    assert(valueParam.EnableFileCompress());

    bool swapMmapOpen =
        kvIndexConfig->GetUseSwapMmapFile() && kvDir->GetStorageType(KV_VALUE_FILE_NAME) == file_system::FSST_MEM;
    file_system::ReaderOption readerOption =
        swapMmapOpen ? file_system::ReaderOption::SwapMmap() : file_system::ReaderOption(file_system::FSOT_LOAD_CONFIG);
    readerOption.supportCompress = true;
    mCompressValueReader = DYNAMIC_POINTER_CAST(file_system::CompressFileReader,
                                                kvDir->CreateFileReader(KV_VALUE_FILE_NAME, readerOption));

    assert(mCompressValueReader);
    const file_system::CompressFileInfoPtr& compressInfo = mCompressValueReader->GetCompressInfo();
    if (valueParam.GetFileCompressType() != compressInfo->compressorName) {
        INDEXLIB_FATAL_ERROR(InconsistentState, "file compressor not equal: schema [%s], index [%s]",
                             valueParam.GetFileCompressType().c_str(), compressInfo->compressorName.c_str());
    }
    return true;
}

bool HashTableCompressVarSegmentReader::doCollectAllCode()
{
    INIT_CODEGEN_INFO(HashTableCompressVarSegmentReader, true);
    mOffsetReader.collectAllCode();
    COLLECT_TYPE_REDEFINE(OffsetReaderType, mOffsetReader.getTargetClassName());
    combineCodegenInfos(&mOffsetReader);
    return true;
}

void HashTableCompressVarSegmentReader::TEST_collectCodegenResult(CodegenCheckers& checkers, std::string id)
{
    codegen::CodegenCheckerPtr checker(new codegen::CodegenChecker);
    COLLECT_TYPE_DEFINE(checker, OffsetReaderType);
    checkers[string("HashTableCompressVarSegmentReader") + id] = checker;
    mOffsetReader.TEST_collectCodegenResult(checkers, id);
}
}} // namespace indexlib::index
