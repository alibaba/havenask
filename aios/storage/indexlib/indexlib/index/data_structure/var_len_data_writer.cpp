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
#include "indexlib/index/data_structure/var_len_data_writer.h"

#include "indexlib/common/field_format/attribute/var_num_attribute_formatter.h"
#include "indexlib/file_system/file/CompressFileWriter.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/index/common/DefaultFSWriterParamDecider.h"
#include "indexlib/util/KeyHasherTyped.h"
using namespace std;
using namespace autil;
using namespace autil::mem_pool;
using namespace indexlib::common;
using namespace indexlib::util;
using namespace indexlib::file_system;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, VarLenDataWriter);

VarLenDataWriter::VarLenDataWriter(PoolBase* pool)
    : mDataItemCount(0)
    , mCurrentOffset(0)
    , mMaxItemLen(0)
    , mDumpPool(pool)
    , mOffsetDumper(mDumpPool)
{
    assert(mDumpPool);
    mOffsetMapPool = new autil::mem_pool::Pool;
}

VarLenDataWriter::~VarLenDataWriter() { DELETE_AND_SET_NULL(mOffsetMapPool); }

void VarLenDataWriter::Init(const DirectoryPtr& directory, const string& offsetFileName, const string& dataFileName,
                            const FSWriterParamDeciderPtr& fsWriterParamDecider, const VarLenDataParam& dumpParam)
{
    FSWriterParamDeciderPtr paramDecider =
        fsWriterParamDecider ? fsWriterParamDecider : FSWriterParamDeciderPtr(new DefaultFSWriterParamDecider);
    file_system::WriterOption writerOption = paramDecider->MakeParam(dataFileName);
    dumpParam.SyncCompressParam(writerOption);
    file_system::FileWriterPtr dataFile = directory->CreateFileWriter(dataFileName, writerOption);

    writerOption = paramDecider->MakeParam(offsetFileName);
    dumpParam.SyncCompressParam(writerOption);
    file_system::FileWriterPtr offsetFile = directory->CreateFileWriter(offsetFileName, writerOption);
    Init(offsetFile, dataFile, dumpParam);
}

void VarLenDataWriter::Init(const FileWriterPtr& offsetFile, const FileWriterPtr& dataFile,
                            const VarLenDataParam& dumpParam)
{
    mOffsetDumper.Init(dumpParam.enableAdaptiveOffset, dumpParam.equalCompressOffset, dumpParam.offsetThreshold);

    mOutputParam = dumpParam;
    mOffsetWriter = offsetFile;
    mDataWriter = dataFile;

    if (!dumpParam.dataCompressorName.empty()) {
        CompressFileWriterPtr compressWriter = DYNAMIC_POINTER_CAST(CompressFileWriter, dataFile);
        if (compressWriter && compressWriter->GetCompressorName() != dumpParam.dataCompressorName) {
            INDEXLIB_FATAL_ERROR(InconsistentState, "data file writer not match compressorName [%s]",
                                 dumpParam.dataCompressorName.c_str());
        }
        compressWriter = DYNAMIC_POINTER_CAST(CompressFileWriter, offsetFile);
        if (compressWriter && compressWriter->GetCompressorName() != dumpParam.dataCompressorName) {
            INDEXLIB_FATAL_ERROR(InconsistentState, "offset file writer not match compressorName [%s]",
                                 dumpParam.dataCompressorName.c_str());
        }
    }

    mDataItemCount = 0;
    mCurrentOffset = 0;
    mMaxItemLen = 0;

    if (dumpParam.dataItemUniqEncode) {
        AllocatorType allocator(mOffsetMapPool);
        mOffsetMap.reset(new OffsetMap(10, OffsetMap::hasher(), OffsetMap::key_equal(), allocator));
    }
}

void VarLenDataWriter::Reserve(uint32_t docCount) { mOffsetDumper.Reserve(docCount + 1); }

void VarLenDataWriter::AppendValue(const StringView& value)
{
    uint64_t hash = GetHashValue(value);
    AppendValue(value, hash);
}

void VarLenDataWriter::AppendValue(const StringView& value, uint64_t hash)
{
    uint64_t offset = AppendValueWithoutOffset(value, hash);
    mOffsetDumper.PushBack(offset);
}

void VarLenDataWriter::Close()
{
    mOffsetMap.reset();
    DELETE_AND_SET_NULL(mOffsetMapPool);
    if (!mDataWriter || !mOffsetWriter) {
        return;
    }

    mDataWriter->Close().GetOrThrow();
    if (!mOutputParam.disableGuardOffset) {
        mOffsetDumper.PushBack(mCurrentOffset);
    }
    if (mOffsetDumper.Size() > 0) {
        mOffsetDumper.Dump(mOffsetWriter);
        mOffsetDumper.Clear();
    }
    mOffsetWriter->Close().GetOrThrow();
}

void VarLenDataWriter::WriteOneDataItem(const StringView& value)
{
    assert(mDataWriter);
    size_t ret = 0;
    if (mOutputParam.appendDataItemLength) {
        char buffer[10];
        ret = VarNumAttributeFormatter::EncodeCount(value.size(), buffer, 10);
        mDataWriter->Write(buffer, ret).GetOrThrow();
    }

    if (value.size() > 0) {
        mDataWriter->Write(value.data(), value.size()).GetOrThrow();
    }
    ret += value.size();
    ++mDataItemCount;
    mMaxItemLen = std::max(mMaxItemLen, ret);
    mCurrentOffset += ret;
}

uint64_t VarLenDataWriter::GetHashValue(const StringView& data)
{
    uint64_t hashValue = 0;
    if (mOffsetMap) {
        MurmurHasher::GetHashKey(data, hashValue);
    }
    return hashValue;
}

void VarLenDataWriter::SetOffset(size_t pos, uint64_t offset) { mOffsetDumper.SetOffset(pos, offset); }

uint64_t VarLenDataWriter::AppendValueWithoutOffset(const StringView& value, uint64_t hash)
{
    uint64_t offset = mCurrentOffset;
    if (!mOffsetMap) {
        WriteOneDataItem(value);
        return offset;
    }

    OffsetMap::const_iterator offsetIter = mOffsetMap->find(hash);
    if (offsetIter != mOffsetMap->end()) {
        offset = offsetIter->second;
    } else {
        WriteOneDataItem(value);
        mOffsetMap->insert(std::make_pair(hash, offset));
    }
    return offset;
}

void VarLenDataWriter::AppendOffset(uint64_t offset) { mOffsetDumper.PushBack(offset); }
}} // namespace indexlib::index
