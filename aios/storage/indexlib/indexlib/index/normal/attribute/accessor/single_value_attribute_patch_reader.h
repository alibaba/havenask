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

#include "indexlib/common_define.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/index/normal/attribute/accessor/attribute_patch_reader.h"
#include "indexlib/index/normal/attribute/format/single_value_attribute_patch_formatter.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/Exception.h"

namespace indexlib { namespace index {

class SinglePatchFile
{
public:
    // TODO: DELETE, legacy for merge
    void Open(const std::string& filePath);

public:
    SinglePatchFile(segmentid_t segmentId, bool patchCompress, bool supportNull, int64_t recordSize);
    ~SinglePatchFile();

    void Open(const file_system::DirectoryPtr& directory, const std::string& fileName);

    bool HasNext();
    template <typename T>
    void Next();
    int64_t GetFileLength() const { return mFileLength; }
    template <typename T>
    void GetPatchValue(T& value, bool& isNull);
    docid_t GetCurDocId() const { return mDocId; }
    segmentid_t GetSegmentId() const { return mSegmentId; }
    uint32_t GetPatchItemCount() const { return mFormatter->GetPatchItemCount(); }

private:
    int64_t mFileLength;

    file_system::FileReaderPtr mFile;
    SingleValueAttributePatchFormatterPtr mFormatter;
    docid_t mDocId;
    bool mIsNull;
    uint64_t mValue;
    segmentid_t mSegmentId;
    bool mPatchCompressed;
    bool mSupportNull;
    int64_t mRecordSize;

private:
    IE_LOG_DECLARE();
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
    typedef std::priority_queue<SinglePatchFile*, std::vector<SinglePatchFile*>, PatchComparator> PatchHeap;

public:
    SingleValueAttributePatchReader(const config::AttributeConfigPtr& attrConfig);
    virtual ~SingleValueAttributePatchReader();

public:
    void AddPatchFile(const file_system::DirectoryPtr& directory, const std::string& fileName,
                      segmentid_t srcSegmentId) override;

    size_t Next(docid_t& docId, uint8_t* buffer, size_t bufferLen, bool& isNull) override;

    size_t Seek(docid_t docId, uint8_t* value, size_t maxLen) override;
    size_t Seek(docid_t docId, uint8_t* value, size_t maxLen, bool& isNull);

    docid_t GetCurrentDocId() const override
    {
        assert(HasNext());
        SinglePatchFile* patchFile = mPatchHeap.top();
        return patchFile->GetCurDocId();
    }

    bool HasNext() const override { return !mPatchHeap.empty(); }

    uint32_t GetMaxPatchItemLen() const override { return sizeof(T); }
    size_t GetPatchFileLength() const override { return mPatchFlieLength; };
    size_t GetPatchItemCount() const override { return mPatchItemCount; }

    bool ReadPatch(docid_t docId, T& value, bool& isNull);
    bool ReadPatch(docid_t docId, uint8_t* value, size_t size, bool& isNull);

protected:
    inline bool InnerReadPatch(docid_t docId, T& value, bool& isNull) __attribute__((always_inline));

private:
    inline void PushBackToHeap(std::unique_ptr<SinglePatchFile>&&) __attribute__((always_inline));

protected:
    PatchHeap mPatchHeap;
    size_t mPatchFlieLength;
    size_t mPatchItemCount;

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP_TEMPLATE(index, SingleValueAttributePatchReader);

///////////////////////////////////////////////////////

inline bool SinglePatchFile::HasNext() { return mFormatter->HasNext(); }

template <typename T>
inline void SinglePatchFile::Next()
{
    T* value = (T*)&mValue;
    mFormatter->Read<T>(mDocId, *value, mIsNull);
}

template <typename T>
inline void SinglePatchFile::GetPatchValue(T& value, bool& isNull)
{
    isNull = mIsNull;
    if (!mIsNull) {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wstrict-aliasing"
        value = *(T*)&mValue;
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
SingleValueAttributePatchReader<T>::SingleValueAttributePatchReader(const config::AttributeConfigPtr& attrConfig)
    : AttributePatchReader(attrConfig)
    , mPatchFlieLength(0)
    , mPatchItemCount(0)
{
}

template <typename T>
SingleValueAttributePatchReader<T>::~SingleValueAttributePatchReader()
{
    while (HasNext()) {
        SinglePatchFile* patchFile = mPatchHeap.top();
        mPatchHeap.pop();
        delete patchFile;
    }
}

template <typename T>
size_t SingleValueAttributePatchReader<T>::Next(docid_t& docId, uint8_t* buffer, size_t bufferLen, bool& isNull)
{
    if (unlikely(bufferLen < sizeof(T))) {
        INDEXLIB_FATAL_ERROR(BufferOverflow, "buffer is not big enough!");
    }

    if (!this->HasNext()) {
        return 0;
    }

    std::unique_ptr<SinglePatchFile> patchFile(this->mPatchHeap.top());
    docId = patchFile->GetCurDocId();

    T& value = *(T*)buffer;
    if (this->InnerReadPatch(docId, value, isNull)) {
        patchFile.release();
        return sizeof(T);
    }
    patchFile.release();
    return 0;
}

template <typename T>
size_t SingleValueAttributePatchReader<T>::Seek(docid_t docId, uint8_t* value, size_t maxLen)
{
    bool isNull = false;
    size_t size = Seek(docId, value, maxLen, isNull);
    assert(!isNull);
    return size;
}

template <typename T>
size_t SingleValueAttributePatchReader<T>::Seek(docid_t docId, uint8_t* value, size_t maxLen, bool& isNull)
{
    if (unlikely(maxLen < sizeof(T))) {
        INDEXLIB_FATAL_ERROR(BufferOverflow, "buffer is not big enough!");
    }

    T& data = *(T*)value;
    return ReadPatch(docId, data, isNull) ? sizeof(T) : 0;
}

template <typename T>
void SingleValueAttributePatchReader<T>::AddPatchFile(const file_system::DirectoryPtr& directory,
                                                      const std::string& fileName, segmentid_t srcSegmentId)
{
    IE_LOG(INFO,
           "Add patch file(%s) in dir (%s)"
           " to patch reader.",
           fileName.c_str(), directory->DebugString().c_str());

    std::unique_ptr<SinglePatchFile> patchFile(
        new SinglePatchFile(srcSegmentId, mAttrConfig->GetCompressType().HasPatchCompress(),
                            mAttrConfig->GetFieldConfig()->IsEnableNullField(), sizeof(T)));
    patchFile->Open(directory, fileName);
    mPatchFlieLength += patchFile->GetFileLength();
    mPatchItemCount += patchFile->GetPatchItemCount();
    if (patchFile->HasNext()) {
        patchFile->Next<T>();
        mPatchHeap.push(patchFile.release());
    }
}

template <typename T>
bool SingleValueAttributePatchReader<T>::ReadPatch(docid_t docId, uint8_t* value, size_t size, bool& isNull)
{
    assert(size == sizeof(T));
    return ReadPatch(docId, *(T*)value, isNull);
}

template <typename T>
bool SingleValueAttributePatchReader<T>::ReadPatch(docid_t docId, T& value, bool& isNull)
{
    if (!HasNext()) {
        return false;
    }

    SinglePatchFile* patchFile = mPatchHeap.top();
    if (patchFile->GetCurDocId() > docId) {
        return false;
    }
    return InnerReadPatch(docId, value, isNull);
}

template <typename T>
bool SingleValueAttributePatchReader<T>::InnerReadPatch(docid_t docId, T& value, bool& isNull)
{
    assert(HasNext());
    bool isFind = false;
    while (HasNext()) {
        std::unique_ptr<SinglePatchFile> patchFile(mPatchHeap.top());
        if (patchFile->GetCurDocId() >= docId) {
            if (patchFile->GetCurDocId() == docId) {
                isFind = true;
            }
            patchFile.release();
            break;
        }
        mPatchHeap.pop();
        patchFile->GetPatchValue<T>(value, isNull);
        PushBackToHeap(std::move(patchFile));
    }

    if (!isFind) {
        return false;
    }

    while (HasNext()) {
        std::unique_ptr<SinglePatchFile> patchFile(mPatchHeap.top());
        if (patchFile->GetCurDocId() != docId) {
            patchFile.release();
            break;
        }
        mPatchHeap.pop();
        patchFile->GetPatchValue<T>(value, isNull);
        PushBackToHeap(std::move(patchFile));
    }

    return true;
}

template <typename T>
void SingleValueAttributePatchReader<T>::PushBackToHeap(std::unique_ptr<SinglePatchFile>&& patchFile)
{
    if (!patchFile->HasNext()) {
        patchFile.reset();
    } else {
        patchFile->Next<T>();
        mPatchHeap.push(patchFile.release());
    }
}
}} // namespace indexlib::index
