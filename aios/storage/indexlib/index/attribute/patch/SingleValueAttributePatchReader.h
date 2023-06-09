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
#pragma once
#include <memory>
#include <queue>

#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "indexlib/base/Types.h"
#include "indexlib/config/CompressTypeOption.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/attribute/format/SingleValueAttributePatchFormatter.h"
#include "indexlib/index/attribute/patch/AttributePatchReader.h"

namespace indexlibv2::index {

class SinglePatchFile
{
public:
    SinglePatchFile(segmentid_t segmentId, bool patchCompress, bool supportNull, int64_t recordSize);
    ~SinglePatchFile() = default;

    Status Open(const std::shared_ptr<indexlib::file_system::IDirectory>& dir, const std::string& fileName);

    bool HasNext();

    template <typename T>
    Status Next();

    int64_t GetFileLength() const { return _fileLength; }

    template <typename T>
    void GetPatchValue(T& value, bool& isNull);

    docid_t GetCurDocId() const { return _docId; }

    segmentid_t GetSegmentId() const { return _segmentId; }

    uint32_t GetPatchItemCount() const { return _formatter->GetPatchItemCount(); }

private:
    int64_t _fileLength;
    std::shared_ptr<indexlib::file_system::FileReader> _fileReader;
    std::shared_ptr<SingleValueAttributePatchFormatter> _formatter;
    docid_t _docId;
    bool _isNull;
    uint64_t _value;
    segmentid_t _segmentId;
    bool _isCompress;
    bool _isSupportNull;
    int64_t _recordSize;

private:
    AUTIL_LOG_DECLARE();
};

class PatchComparator
{
public:
    bool operator()(SinglePatchFile*& lft, SinglePatchFile*& rht)
    {
        if (lft->GetCurDocId() > rht->GetCurDocId()) {
            return true;
        } else if (lft->GetCurDocId() < rht->GetCurDocId()) {
            return false;
        }
        return (lft->GetSegmentId() > rht->GetSegmentId());
    }
};

template <typename T>
class SingleValueAttributePatchReader : public AttributePatchReader
{
public:
    using PatchHeap = std::priority_queue<SinglePatchFile*, std::vector<SinglePatchFile*>, PatchComparator>;

public:
    SingleValueAttributePatchReader(const std::shared_ptr<config::AttributeConfig>& attrConfig);
    virtual ~SingleValueAttributePatchReader();

public:
    Status AddPatchFile(const std::shared_ptr<indexlib::file_system::IDirectory>& directory,
                        const std::string& fileName, segmentid_t srcSegmentId) override;

    std::pair<Status, size_t> Next(docid_t& docId, uint8_t* buffer, size_t bufferLen, bool& isNull) override;
    std::pair<Status, bool> Next(docid_t& docId, T& value, bool& isNull);

    std::pair<Status, size_t> Seek(docid_t docId, uint8_t* value, size_t maxLen);
    std::pair<Status, size_t> Seek(docid_t docId, uint8_t* value, size_t maxLen, bool& isNull) override;

    docid_t GetCurrentDocId() const override
    {
        assert(HasNext());
        SinglePatchFile* patchFile = _patchHeap.top();
        return patchFile->GetCurDocId();
    }

    bool HasNext() const override { return !_patchHeap.empty(); }

    uint32_t GetMaxPatchItemLen() const override { return sizeof(T); }
    size_t GetPatchFileLength() const override { return _patchFileLength; };
    size_t GetPatchItemCount() const override { return _patchItemCount; }

    std::pair<Status, bool> ReadPatch(docid_t docId, T& value, bool& isNull);
    std::pair<Status, bool> ReadPatch(docid_t docId, uint8_t* value, size_t size, bool& isNull);

protected:
    inline std::pair<Status, bool> InnerReadPatch(docid_t docId, T& value, bool& isNull) __attribute__((always_inline));

private:
    inline Status PushBackToHeap(std::unique_ptr<SinglePatchFile>&&) __attribute__((always_inline));

protected:
    PatchHeap _patchHeap;
    size_t _patchFileLength;
    size_t _patchItemCount;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, SingleValueAttributePatchReader, T);

///////////////////////////////////////////////////////

inline bool SinglePatchFile::HasNext() { return _formatter->HasNext(); }

template <typename T>
inline Status SinglePatchFile::Next()
{
    T* value = (T*)&_value;
    auto [st, _] = _formatter->Read<T>(_docId, *value, _isNull);
    RETURN_IF_STATUS_ERROR(st, "invalid next operator for single patch file.");
    return Status::OK();
}

template <typename T>
inline void SinglePatchFile::GetPatchValue(T& value, bool& isNull)
{
    isNull = _isNull;
    if (!_isNull) {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wstrict-aliasing"
        value = *(T*)&_value;
#pragma GCC diagnostic pop
    }
}

template <>
inline void SinglePatchFile::GetPatchValue<autil::LongHashValue<2>>(autil::LongHashValue<2>& value, bool& isNull)
{
    // primary key not support update
    assert(false);
}

/////////////////////////////////////////////////////
template <typename T>
SingleValueAttributePatchReader<T>::SingleValueAttributePatchReader(
    const std::shared_ptr<config::AttributeConfig>& attrConfig)
    : AttributePatchReader(attrConfig)
    , _patchFileLength(0)
    , _patchItemCount(0)
{
}

template <typename T>
SingleValueAttributePatchReader<T>::~SingleValueAttributePatchReader()
{
    while (HasNext()) {
        SinglePatchFile* patchFile = _patchHeap.top();
        _patchHeap.pop();
        delete patchFile;
    }
}

template <typename T>
std::pair<Status, size_t> SingleValueAttributePatchReader<T>::Next(docid_t& docId, uint8_t* buffer, size_t bufferLen,
                                                                   bool& isNull)
{
    if (unlikely(bufferLen < sizeof(T))) {
        AUTIL_LOG(ERROR, "buffer is not big enough!");
        return std::make_pair(Status::InternalError("buffer is not big enough!"), 0);
    }

    if (!this->HasNext()) {
        return std::make_pair(Status::OK(), 0);
    }

    std::unique_ptr<SinglePatchFile> patchFile(this->_patchHeap.top());
    docId = patchFile->GetCurDocId();

    T& value = *(T*)buffer;
    auto [status, ret] = this->InnerReadPatch(docId, value, isNull);
    RETURN2_IF_STATUS_ERROR(status, sizeof(T), "read patch info fail");
    if (ret) {
        patchFile.release();
        return std::make_pair(Status::OK(), sizeof(T));
    }
    patchFile.release();
    return std::make_pair(Status::OK(), 0);
}

template <typename T>
std::pair<Status, bool> SingleValueAttributePatchReader<T>::Next(docid_t& docId, T& value, bool& isNull)
{
    if (!this->HasNext()) {
        return std::make_pair(Status::OK(), false);
    }

    SinglePatchFile* patchFile = this->_patchHeap.top();
    docId = patchFile->GetCurDocId();
    return this->InnerReadPatch(docId, value, isNull);
}

template <typename T>
std::pair<Status, size_t> SingleValueAttributePatchReader<T>::Seek(docid_t docId, uint8_t* value, size_t maxLen)
{
    bool isNull = false;
    return Seek(docId, value, maxLen, isNull);
}

template <typename T>
std::pair<Status, size_t> SingleValueAttributePatchReader<T>::Seek(docid_t docId, uint8_t* value, size_t maxLen,
                                                                   bool& isNull)
{
    if (unlikely(maxLen < sizeof(T))) {
        AUTIL_LOG(ERROR, "buffer is not big enough!");
        return std::make_pair(Status::InternalError("buffer is not big enough!"), 0);
    }

    T& data = *(T*)value;
    auto [status, ret] = ReadPatch(docId, data, isNull);
    RETURN2_IF_STATUS_ERROR(status, 0, "read patch info fail.");
    return std::make_pair(Status::OK(), ret ? sizeof(T) : 0);
}

template <typename T>
Status
SingleValueAttributePatchReader<T>::AddPatchFile(const std::shared_ptr<indexlib::file_system::IDirectory>& directory,
                                                 const std::string& fileName, segmentid_t srcSegmentId)
{
    AUTIL_LOG(INFO,
              "Add patch file(%s) in dir (%s)"
              " to patch reader.",
              fileName.c_str(), directory->DebugString().c_str());

    std::unique_ptr<SinglePatchFile> patchFile(
        new SinglePatchFile(srcSegmentId, _attrConfig->GetCompressType().HasPatchCompress(),
                            _attrConfig->GetFieldConfig()->IsEnableNullField(), sizeof(T)));
    auto status = patchFile->Open(directory, fileName);
    RETURN_IF_STATUS_ERROR(status, "open patch file [%s] fail.", fileName.c_str());
    _patchFileLength += patchFile->GetFileLength();
    _patchItemCount += patchFile->GetPatchItemCount();
    if (patchFile->HasNext()) {
        status = patchFile->Next<T>();
        RETURN_IF_STATUS_ERROR(status, "read next patch value fail.");
        _patchHeap.push(patchFile.release());
    }
    return Status::OK();
}

template <typename T>
std::pair<Status, bool> SingleValueAttributePatchReader<T>::ReadPatch(docid_t docId, uint8_t* value, size_t size,
                                                                      bool& isNull)
{
    assert(size == sizeof(T));
    return ReadPatch(docId, *(T*)value, isNull);
}

template <typename T>
std::pair<Status, bool> SingleValueAttributePatchReader<T>::ReadPatch(docid_t docId, T& value, bool& isNull)
{
    if (!HasNext()) {
        return std::make_pair(Status::OK(), false);
    }

    SinglePatchFile* patchFile = _patchHeap.top();
    if (patchFile->GetCurDocId() > docId) {
        return std::make_pair(Status::OK(), false);
    }
    return InnerReadPatch(docId, value, isNull);
}

template <typename T>
std::pair<Status, bool> SingleValueAttributePatchReader<T>::InnerReadPatch(docid_t docId, T& value, bool& isNull)
{
    assert(HasNext());
    bool isFind = false;
    while (HasNext()) {
        std::unique_ptr<SinglePatchFile> patchFile(_patchHeap.top());
        if (patchFile->GetCurDocId() >= docId) {
            if (patchFile->GetCurDocId() == docId) {
                isFind = true;
            }
            patchFile.release();
            break;
        }
        _patchHeap.pop();
        patchFile->GetPatchValue<T>(value, isNull);
        auto status = PushBackToHeap(std::move(patchFile));
        RETURN2_IF_STATUS_ERROR(status, false, "push back to heap fail when read patch info.");
    }

    if (!isFind) {
        return std::make_pair(Status::OK(), false);
    }

    while (HasNext()) {
        std::unique_ptr<SinglePatchFile> patchFile(_patchHeap.top());
        if (patchFile->GetCurDocId() != docId) {
            patchFile.release();
            break;
        }
        _patchHeap.pop();
        patchFile->GetPatchValue<T>(value, isNull);
        auto status = PushBackToHeap(std::move(patchFile));
        RETURN2_IF_STATUS_ERROR(status, false, "push back to heap fail when read patch info.");
    }

    return std::make_pair(Status::OK(), true);
}

template <typename T>
Status SingleValueAttributePatchReader<T>::PushBackToHeap(std::unique_ptr<SinglePatchFile>&& patchFile)
{
    if (!patchFile->HasNext()) {
        patchFile.reset();
    } else {
        auto status = patchFile->Next<T>();
        RETURN_IF_STATUS_ERROR(status, "read next path fail.");
        _patchHeap.push(patchFile.release());
    }
    return Status::OK();
}

} // namespace indexlibv2::index
