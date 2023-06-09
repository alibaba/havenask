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
#include "indexlib/index/normal/attribute/accessor/attribute_offset_reader.h"

#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/ReaderOption.h"
#include "indexlib/file_system/WriterOption.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/file_system/file/SliceFileReader.h"
#include "indexlib/index/data_structure/attribute_compress_info.h"
#include "indexlib/index/data_structure/var_len_data_param_helper.h"
#include "indexlib/index_define.h"

using namespace std;
using namespace indexlib::config;
using namespace indexlib::file_system;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, AttributeOffsetReader);

AttributeOffsetReader::AttributeOffsetReader(const AttributeConfigPtr& attrConfig, AttributeMetrics* attributeMetric)
    : mOffsetReader(VarLenDataParamHelper::MakeParamForAttribute(attrConfig))
    , mAttrConfig(attrConfig)
    , mExpandSliceLen(0)
    , mAttributeMetrics(attributeMetric)
{
}

AttributeOffsetReader::~AttributeOffsetReader() {}

void AttributeOffsetReader::Init(const DirectoryPtr& attrDirectory, uint32_t docCount, bool supportFileCompress,
                                 bool disableUpdate)
{
    FileReaderPtr offsetFileReader;
    SliceFileReaderPtr extFileReader;
    if (disableUpdate) {
        offsetFileReader = InitOffsetFile(attrDirectory, supportFileCompress);
        extFileReader = nullptr;
    } else if (AttributeCompressInfo::NeedCompressOffset(mAttrConfig)) {
        InitCompressOffsetFile(attrDirectory, offsetFileReader, extFileReader, supportFileCompress);
    } else {
        InitUncompressOffsetFile(attrDirectory, docCount, offsetFileReader, extFileReader, supportFileCompress);
    }
    Init(docCount, offsetFileReader, extFileReader);
}

void AttributeOffsetReader::Init(uint32_t docCount, const FileReaderPtr& fileReader,
                                 const SliceFileReaderPtr& sliceFile)
{
    mOffsetReader.Init(docCount, fileReader, sliceFile);
    CompressOffsetReader* reader = mOffsetReader.GetCompressOffsetReader();
    if (reader && sliceFile) {
        mCompressMetrics = reader->GetUpdateMetrics();
        mExpandSliceLen = sliceFile->GetLength();
        InitAttributeMetrics();
    }
}

void AttributeOffsetReader::InitCompressOffsetFile(const DirectoryPtr& attrDirectory, FileReaderPtr& offsetFileReader,
                                                   SliceFileReaderPtr& sliceFileReader, bool supportFileCompress)
{
    offsetFileReader = InitOffsetFile(attrDirectory, supportFileCompress);
    if (!mAttrConfig->IsAttributeUpdatable() || !offsetFileReader->GetBaseAddress()) {
        // not allowed to update if offset is open in block cache
        sliceFileReader = SliceFileReaderPtr();
        return;
    }

    string extendFileName = string(ATTRIBUTE_OFFSET_FILE_NAME) + ATTRIBUTE_EQUAL_COMPRESS_UPDATE_EXTEND_SUFFIX;
    FileReaderPtr fileReader = attrDirectory->CreateFileReader(extendFileName, FSOT_SLICE);
    if (!fileReader) {
        auto fileWriter =
            attrDirectory->CreateFileWriter(extendFileName, WriterOption::Slice(EQUAL_COMPRESS_UPDATE_SLICE_LEN,
                                                                                EQUAL_COMPRESS_UPDATE_MAX_SLICE_COUNT));
        fileReader = attrDirectory->CreateFileReader(extendFileName, FSOT_SLICE);
        fileWriter->Close().GetOrThrow();
    }
    sliceFileReader = DYNAMIC_POINTER_CAST(SliceFileReader, fileReader);
}

void AttributeOffsetReader::InitUncompressOffsetFile(const DirectoryPtr& attrDirectory, uint32_t docCount,
                                                     FileReaderPtr& offsetFileReader,
                                                     SliceFileReaderPtr& sliceFileReader, bool supportFileCompress)
{
    if (!mAttrConfig->IsAttributeUpdatable()) {
        offsetFileReader = InitOffsetFile(attrDirectory, supportFileCompress);
        sliceFileReader = SliceFileReaderPtr();
        return;
    }

    size_t u64OffsetFileLen = sizeof(uint64_t) * (docCount + 1);
    string extendFileName = string(ATTRIBUTE_OFFSET_FILE_NAME) + ATTRIBUTE_OFFSET_EXTEND_SUFFIX;

    FileReaderPtr fileReader = attrDirectory->CreateFileReader(extendFileName, FSOT_SLICE);
    if (!fileReader) {
        auto fileWriter = attrDirectory->CreateFileWriter(extendFileName, WriterOption::Slice(u64OffsetFileLen, 1));
        fileReader = attrDirectory->CreateFileReader(extendFileName, FSOT_SLICE);
        fileWriter->Close().GetOrThrow();
    }
    size_t fileLen = fileReader->GetLength();
    if (fileLen > 0) {
        // already extend to u64
        offsetFileReader = fileReader;
        sliceFileReader = SliceFileReaderPtr();
    } else {
        offsetFileReader = InitOffsetFile(attrDirectory, supportFileCompress);
        if (offsetFileReader->GetBaseAddress()) {
            sliceFileReader = DYNAMIC_POINTER_CAST(SliceFileReader, fileReader);
        } else {
            // not allowed to update if offset is open in block cache
            sliceFileReader = SliceFileReaderPtr();
        }
    }
}

FileReaderPtr AttributeOffsetReader::InitOffsetFile(const DirectoryPtr& attrDirectory, bool supportFileCompress)
{
    ReaderOption option = ReaderOption::Writable(FSOT_LOAD_CONFIG);
    option.supportCompress = supportFileCompress;
    return attrDirectory->CreateFileReader(ATTRIBUTE_OFFSET_FILE_NAME, option);
}

void AttributeOffsetReader::InitAttributeMetrics()
{
    if (!mAttributeMetrics) {
        return;
    }
    mAttributeMetrics->IncreaseEqualCompressExpandFileLenValue(mExpandSliceLen);
    mAttributeMetrics->IncreaseEqualCompressWastedBytesValue(mCompressMetrics.noUsedBytesSize);
}
}} // namespace indexlib::index
