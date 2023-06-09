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
#ifndef __INDEXLIB_VAR_LEN_DATA_READER_H
#define __INDEXLIB_VAR_LEN_DATA_READER_H

#include <memory>

#include "autil/ConstString.h"
#include "indexlib/common/field_format/attribute/var_num_attribute_formatter.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system//file/CompressFileReader.h"
#include "indexlib/file_system//file/FileReader.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/stream/FileStream.h"
#include "indexlib/file_system/stream/FileStreamCreator.h"
#include "indexlib/index/data_structure/var_len_offset_reader.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class VarLenDataReader
{
public:
    VarLenDataReader(const VarLenDataParam& param, bool isOnline);
    ~VarLenDataReader();

public:
    void Init(uint32_t docCount, const file_system::DirectoryPtr& directory, const std::string& offsetFileName,
              const std::string& dataFileName);

    inline uint64_t GetOffset(docid_t docId) const
    {
        if (mIsOnline) {
            return mOffsetReader.GetOffset(docId);
        }
        return mOfflineOffsetReader.GetOffset(docId);
    }

    inline uint32_t GetDocCount() const { return mOffsetReader.GetDocCount(); }

    inline bool GetValue(docid_t docId, autil::StringView& value,
                         autil::mem_pool::PoolBase* pool) const __ALWAYS_INLINE;

    future_lite::coro::Lazy<index::ErrorCodeVec> GetValue(const std::vector<docid_t>& docIds,
                                                          autil::mem_pool::PoolBase* pool,
                                                          file_system::ReadOption readOption,
                                                          std::vector<autil::StringView>* data) const noexcept;

    const file_system::FileReaderPtr& GetOffsetFileReader() const { return mOffsetReader.GetFileReader(); }

    const file_system::FileReaderPtr& GetDataFileReader() const { return mDataFileReader; }

private:
    inline bool GetOffsetAndLength(file_system::FileReader* fileReader, docid_t docId, uint64_t& offset,
                                   uint32_t& length) const __ALWAYS_INLINE;

    inline bool GetValue(file_system::FileReader* fileReader, docid_t docId, autil::StringView& value,
                         autil::mem_pool::PoolBase* pool) const __ALWAYS_INLINE;

    future_lite::coro::Lazy<index::ErrorCodeVec>
    GetOffsetAndLength(const std::shared_ptr<file_system::FileStream>& fileStream, const std::vector<docid_t>& docIds,
                       autil::mem_pool::PoolBase* sessionPool, file_system::ReadOption readOption,
                       std::vector<uint64_t>* offsets, std::vector<uint32_t>* lens) const noexcept;

private:
    autil::mem_pool::Pool mOfflinePool;
    VarLenOffsetReader mOffsetReader;
    VarLenOffsetReader mOfflineOffsetReader;
    file_system::FileReaderPtr mDataFileReader;
    char* mDataBaseAddr;
    size_t mDataLength;
    VarLenDataParam mParam;
    bool mIsOnline;
    bool mDataFileCompress;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(VarLenDataReader);
///////////////////////////////////////////////////////////////////////////////////////
inline bool VarLenDataReader::GetValue(docid_t docId, autil::StringView& value, autil::mem_pool::PoolBase* pool) const
{
    if (!mDataFileCompress) {
        file_system::FileReader* dataReader = mDataFileReader.get();
        return GetValue(dataReader, docId, value, pool);
    }
    autil::mem_pool::Pool* sessionPool = dynamic_cast<autil::mem_pool::Pool*>(pool);
    if (!sessionPool) {
        IE_LOG(ERROR, "read value fail, pool should not be null for compressed data.");
        return false;
    }
    file_system::CompressFileReaderPtr compReader =
        std::static_pointer_cast<file_system::CompressFileReader>(mDataFileReader);
    if (!mIsOnline) {
        return GetValue(compReader.get(), docId, value, pool);
    }
    file_system::CompressFileReader* dataReader = compReader->CreateSessionReader(sessionPool);
    file_system::CompressFileReaderGuard readerCuard(dataReader, sessionPool);
    return GetValue(dataReader, docId, value, pool);
}

inline bool VarLenDataReader::GetValue(file_system::FileReader* dataReader, docid_t docId, autil::StringView& value,
                                       autil::mem_pool::PoolBase* pool) const
{
    uint64_t offset = 0;
    uint32_t len = 0;
    if (!GetOffsetAndLength(dataReader, docId, offset, len)) {
        return false;
    }

    if (mDataBaseAddr) {
        assert(!mDataFileCompress);
        value = autil::StringView(mDataBaseAddr + offset, len);
        return true;
    }

    if (!pool) {
        IE_LOG(ERROR, "read value fail, pool should not be null.");
        return false;
    }

    char* buffer = (char*)pool->allocate(len);
    size_t retLen = dataReader->Read(buffer, len, offset).GetOrThrow();
    if (retLen != (size_t)len) {
        IE_LOG(ERROR, "read value fail from file [%s], offset [%lu], len [%u]", dataReader->DebugString().c_str(),
               offset, len);
        return false;
    }
    value = autil::StringView(buffer, len);
    return true;
}

inline bool VarLenDataReader::GetOffsetAndLength(file_system::FileReader* dataReader, docid_t docId, uint64_t& offset,
                                                 uint32_t& length) const
{
    if (docId >= GetDocCount()) {
        IE_LOG(ERROR, "invalid docId [%d], over docCount [%u]", docId, GetDocCount());
        return false;
    }

    if (mParam.dataItemUniqEncode && !mParam.appendDataItemLength) {
        IE_LOG(ERROR, "var_len data structure not support enable dataItemUniqEncode, "
                      "but not set appendDataItemLength");
        return false;
    }

    offset = GetOffset(docId);
    if (!mParam.appendDataItemLength) {
        assert(!mParam.dataItemUniqEncode);
        uint64_t nextOffset = 0;
        if (unlikely(mParam.disableGuardOffset && (docId + 1) == (docid_t)GetDocCount())) {
            nextOffset = mDataLength;
        } else {
            nextOffset = GetOffset(docId + 1);
        }
        length = nextOffset - offset;
    } else {
        size_t encodeCountLen = 0;
        bool isNull = false;
        if (mDataBaseAddr) {
            char* buffPtr = mDataBaseAddr + offset;
            length = common::VarNumAttributeFormatter::DecodeCount((const char*)buffPtr, encodeCountLen, isNull);
        } else {
            char buffPtr[5];
            size_t retLen = dataReader->Read(buffPtr, sizeof(uint8_t), offset).GetOrThrow();
            if (retLen != sizeof(uint8_t)) {
                return false;
            }
            encodeCountLen = common::VarNumAttributeFormatter::GetEncodedCountFromFirstByte(*buffPtr);
            // read remain bytes for count
            retLen =
                dataReader->Read(buffPtr + sizeof(uint8_t), encodeCountLen - 1, offset + sizeof(uint8_t)).GetOrThrow();
            if (retLen != encodeCountLen - 1) {
                return false;
            }
            length = common::VarNumAttributeFormatter::DecodeCount((const char*)buffPtr, encodeCountLen, isNull);
        }
        offset += encodeCountLen;
    }
    return true;
}

inline future_lite::coro::Lazy<index::ErrorCodeVec>
VarLenDataReader::GetOffsetAndLength(const std::shared_ptr<file_system::FileStream>& fileStream,
                                     const std::vector<docid_t>& docIds, autil::mem_pool::PoolBase* sessionPool,
                                     file_system::ReadOption readOption, std::vector<uint64_t>* offsets,
                                     std::vector<uint32_t>* lens) const noexcept
{
    if (docIds.empty()) {
        co_return index::ErrorCodeVec();
    }
    lens->resize(docIds.size());
    index::ErrorCodeVec result(docIds.size(), index::ErrorCode::OK);
    uint32_t docCount = GetDocCount();
    for (size_t i = 0; i < docIds.size(); ++i) {
        if (docIds[i] >= docCount) {
            IE_LOG(ERROR, "invalid docId [%d], over docCount [%u]", docIds[i], docCount);
            co_return index::ErrorCodeVec(docIds.size(), index::ErrorCode::Runtime);
        }
    }

    if (mParam.dataItemUniqEncode && !mParam.appendDataItemLength) {
        IE_LOG(ERROR, "var_len data structure not support enable dataItemUniqEncode, "
                      "but not set appendDataItemLength");
        co_return index::ErrorCodeVec(docIds.size(), index::ErrorCode::Runtime);
    }
    auto sessionOffsetReader = mOffsetReader.CreateSessionReader(dynamic_cast<autil::mem_pool::Pool*>(sessionPool));
    if (!mParam.appendDataItemLength) {
        assert(!mParam.dataItemUniqEncode);
        std::vector<docid_t> targetDocIds;
        for (size_t i = 0; i < docIds.size(); ++i) {
            targetDocIds.push_back(docIds[i]);
            targetDocIds.push_back(docIds[i] + 1);
        }
        if (mParam.disableGuardOffset && (docIds[docIds.size() - 1] + 1 == (docid_t)GetDocCount())) {
            targetDocIds.pop_back();
        }

        std::vector<uint64_t> targetOffsets;
        auto offsetResult = co_await sessionOffsetReader.GetOffset(targetDocIds, readOption, &targetOffsets);
        if (mParam.disableGuardOffset && (docIds[docIds.size() - 1] + 1 == (docid_t)GetDocCount())) {
            // append one offset to  make 2n offset, make code easy
            targetDocIds.push_back(docIds[docIds.size() - 1] + 1);
            targetOffsets.push_back(mDataLength);
            offsetResult.push_back(index::ErrorCode::OK);
        }
        for (size_t i = 0; i < docIds.size(); ++i) {
            offsets->push_back(targetOffsets[2 * i]);
            (*lens)[i] = (targetOffsets[2 * i + 1] - targetOffsets[2 * i]);
            if (index::ErrorCode::OK != offsetResult[2 * i] || index::ErrorCode::OK != offsetResult[2 * i + 1]) {
                result[i] = index::ErrorCode::Runtime;
            }
        }
        co_return result;
    } else {
        auto offsetResult = co_await sessionOffsetReader.GetOffset(docIds, readOption, offsets);
        bool isNull;
        if (mDataBaseAddr) {
            for (size_t i = 0; i < docIds.size(); ++i) {
                if (offsetResult[i] == index::ErrorCode::OK) {
                    size_t encodeCountLen = 0;
                    char* buffPtr = mDataBaseAddr + (*offsets)[i];
                    (*lens)[i] =
                        common::VarNumAttributeFormatter::DecodeCount((const char*)buffPtr, encodeCountLen, isNull);
                    (*offsets)[i] += encodeCountLen;
                } else {
                    result[i] = offsetResult[i];
                }
            }
        } else {
            char* batchBufferPtr = IE_POOL_COMPATIBLE_NEW_VECTOR(sessionPool, char, 5 * docIds.size());
            file_system::BatchIO batchIO(docIds.size());
            size_t bufferIdx = 0;
            size_t streamLen = fileStream->GetStreamLength();
            for (size_t i = 0; i < docIds.size(); ++i) {
                assert(streamLen >= (*offsets)[i]);
                size_t len = std::min(size_t(5), streamLen - (*offsets)[i]);
                if (offsetResult[i] == index::ErrorCode::OK) {
                    batchIO[bufferIdx] = file_system::SingleIO(batchBufferPtr + bufferIdx * 5, len, (*offsets)[i]);
                    bufferIdx++;
                }
            }
            batchIO.resize(bufferIdx);
            auto ioResult = co_await fileStream->BatchRead(batchIO, readOption);
            assert(ioResult.size() == batchIO.size());
            bufferIdx = 0;
            for (size_t i = 0; i < docIds.size(); ++i) {
                if (index::ErrorCode::OK != offsetResult[i]) {
                    continue;
                }
                if (!ioResult[bufferIdx].OK()) {
                    result[i] = index::ConvertFSErrorCode(ioResult[bufferIdx].ec);
                    bufferIdx++;
                    continue;
                }
                assert(bufferIdx < batchIO.size());
                char* bufferPtr = (char*)batchIO[bufferIdx].buffer;
                size_t encodeCountLen = common::VarNumAttributeFormatter::GetEncodedCountFromFirstByte(*bufferPtr);
                // read remain bytes for count
                assert(bufferPtr);
                (*lens)[i] =
                    common::VarNumAttributeFormatter::DecodeCount((const char*)bufferPtr, encodeCountLen, isNull);
                (*offsets)[i] += encodeCountLen;
                bufferIdx++;
            }
            IE_POOL_COMPATIBLE_DELETE_VECTOR(sessionPool, batchBufferPtr, 5 * docIds.size());
        }
        co_return result;
    }
    assert(false);
    co_return index::ErrorCodeVec();
}

inline future_lite::coro::Lazy<index::ErrorCodeVec>
VarLenDataReader::GetValue(const std::vector<docid_t>& docIds, autil::mem_pool::PoolBase* pool,
                           file_system::ReadOption readOption, std::vector<autil::StringView>* data) const noexcept
{
    if (!std::is_sorted(docIds.begin(), docIds.end())) {
        IE_LOG(ERROR, "read value fail, docids is not increasing.");
        co_return index::ErrorCodeVec(docIds.size(), index::ErrorCode::Runtime);
    }
    assert(mIsOnline);
    if (!pool) {
        IE_LOG(ERROR, "read value fail, pool is required.");
        co_return index::ErrorCodeVec(docIds.size(), index::ErrorCode::Runtime);
    }
    index::ErrorCodeVec ret(docIds.size(), index::ErrorCode::OK);
    std::vector<uint64_t> offsets;
    std::vector<uint32_t> lens;
    std::shared_ptr<file_system::FileStream> fileStream;
    if (!mDataBaseAddr) {
        fileStream = file_system::FileStreamCreator::CreateFileStream(mDataFileReader,
                                                                      dynamic_cast<autil::mem_pool::Pool*>(pool));
    }

    auto offsetResult = co_await GetOffsetAndLength(fileStream, docIds, pool, readOption, &offsets, &lens);

    if (mDataBaseAddr) {
        assert(!mDataFileCompress);
        for (size_t i = 0; i < offsetResult.size(); ++i) {
            if (offsetResult[i] == index::ErrorCode::OK) {
                data->push_back(autil::StringView(mDataBaseAddr + offsets[i], lens[i]));
            } else {
                data->push_back(autil::StringView());
                ret[i] = offsetResult[i];
            }
        }
        co_return ret;
    }

    if (!pool) {
        IE_LOG(ERROR, "read value fail, pool should not be null.");
        co_return index::ErrorCodeVec(docIds.size(), index::ErrorCode::Runtime);
    }
    file_system::BatchIO batchIO;
    for (size_t i = 0; i < docIds.size(); ++i) {
        if (index::ErrorCode::OK == offsetResult[i]) {
            batchIO.emplace_back(pool->allocate(lens[i]), lens[i], offsets[i]);
        }
    }
    auto dataReadResult = co_await fileStream->BatchRead(batchIO, readOption);
    size_t resultIdx = 0;
    for (size_t i = 0; i < docIds.size(); ++i) {
        if (offsetResult[i] == index::ErrorCode::OK) {
            if (dataReadResult[resultIdx].OK()) {
                data->push_back(autil::StringView((char*)batchIO[resultIdx].buffer, batchIO[resultIdx].len));
            } else {
                ret[i] = index::ConvertFSErrorCode(dataReadResult[resultIdx].ec);
                data->push_back(autil::StringView());
            }
            resultIdx++;
        } else {
            ret[i] = offsetResult[i];
            data->push_back(autil::StringView());
        }
    }
    co_return ret;
}

}} // namespace indexlib::index

#endif //__INDEXLIB_VAR_LEN_DATA_READER_H
