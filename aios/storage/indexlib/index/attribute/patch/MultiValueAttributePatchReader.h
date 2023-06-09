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
#include "autil/MultiValueType.h"
#include "autil/NoCopyable.h"
#include "indexlib/base/Types.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/attribute/patch/AttributePatchReader.h"
#include "indexlib/index/attribute/patch/MultiValueAttributePatchFile.h"

namespace indexlibv2::index {

class MultiValueAttributePatchComparator
{
public:
    bool operator()(MultiValueAttributePatchFile*& lft, MultiValueAttributePatchFile*& rht)
    {
        if (lft->GetCurDocId() > rht->GetCurDocId()) {
            return true;
        }
        if (lft->GetCurDocId() < rht->GetCurDocId()) {
            return false;
        }
        return (lft->GetSegmentId() < rht->GetSegmentId());
    }
};

template <typename T>
class MultiValueAttributePatchReader : public AttributePatchReader
{
public:
    using PatchHeap = std::priority_queue<MultiValueAttributePatchFile*, std::vector<MultiValueAttributePatchFile*>,
                                          MultiValueAttributePatchComparator>;

public:
    MultiValueAttributePatchReader(const std::shared_ptr<config::AttributeConfig>& attrConfig);
    ~MultiValueAttributePatchReader();

public:
    Status AddPatchFile(const std::shared_ptr<indexlib::file_system::IDirectory>& directory,
                        const std::string& fileName, segmentid_t srcSegmentId) override;

    std::pair<Status, size_t> Next(docid_t& docId, uint8_t* value, size_t maxLen, bool& isNull) override;

    bool HasNext() const override { return !_patchHeap.empty(); }

    docid_t GetCurrentDocId() const override
    {
        assert(HasNext());
        MultiValueAttributePatchFile* patchFile = _patchHeap.top();
        return patchFile->GetCurDocId();
    }

    std::pair<Status, size_t> Seek(docid_t docId, uint8_t* value, size_t maxLen, bool&) override;
    std::pair<Status, size_t> Seek(docid_t docId, uint8_t* value, size_t maxLen);
    uint32_t GetMaxPatchItemLen() const override { return _maxPatchValueLen; }
    size_t GetPatchFileLength() const override { return _patchFileLength; };
    size_t GetPatchItemCount() const override { return _patchItemCount; }

protected:
    Status SkipCurDocValue(docid_t docId);

private:
    Status PushBackToHeap(std::unique_ptr<MultiValueAttributePatchFile>&&);
    MultiValueAttributePatchFile* CreatePatchFile(segmentid_t segmentId);

protected:
    PatchHeap _patchHeap;
    uint32_t _maxPatchValueLen;
    size_t _patchFileLength;
    size_t _patchItemCount;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, MultiValueAttributePatchReader, T);

/////////////////////////////////////////////////////
template <typename T>
MultiValueAttributePatchReader<T>::MultiValueAttributePatchReader(
    const std::shared_ptr<config::AttributeConfig>& attrConfig)
    : AttributePatchReader(attrConfig)
    , _maxPatchValueLen(0)
    , _patchFileLength(0)
    , _patchItemCount(0)
{
}

template <typename T>
MultiValueAttributePatchReader<T>::~MultiValueAttributePatchReader()
{
    while (HasNext()) {
        MultiValueAttributePatchFile* patchFile = _patchHeap.top();
        _patchHeap.pop();
        delete patchFile;
    }
}

template <typename T>
Status
MultiValueAttributePatchReader<T>::AddPatchFile(const std::shared_ptr<indexlib::file_system::IDirectory>& directory,
                                                const std::string& fileName, segmentid_t srcSegmentId)
{
    AUTIL_LOG(DEBUG,
              "Add patch file(%s) in dir (%s)"
              " to patch reader.",
              fileName.c_str(), directory->DebugString().c_str());

    std::unique_ptr<MultiValueAttributePatchFile> patchFile(CreatePatchFile(srcSegmentId));
    auto st = patchFile->Open(directory, fileName);
    RETURN_IF_STATUS_ERROR(st, "open patch file failed.");
    _patchFileLength += patchFile->GetFileLength();
    _patchItemCount += patchFile->GetPatchItemCount();
    if (patchFile->HasNext()) {
        st = patchFile->Next();
        RETURN_IF_STATUS_ERROR(st, "invalid patch file next.");
        _maxPatchValueLen = std::max(_maxPatchValueLen, patchFile->GetMaxPatchItemLen());
        _patchHeap.push(patchFile.release());
    }
    return Status::OK();
}

template <typename T>
MultiValueAttributePatchFile* MultiValueAttributePatchReader<T>::CreatePatchFile(segmentid_t segmentId)
{
    return new MultiValueAttributePatchFile(segmentId, _attrConfig);
}

template <typename T>
std::pair<Status, size_t> MultiValueAttributePatchReader<T>::Seek(docid_t docId, uint8_t* value, size_t maxLen, bool&)
{
    return Seek(docId, value, maxLen);
}

template <typename T>
std::pair<Status, size_t> MultiValueAttributePatchReader<T>::Seek(docid_t docId, uint8_t* value, size_t maxLen)
{
    while (HasNext()) {
        std::unique_ptr<MultiValueAttributePatchFile> patchFile(_patchHeap.top());
        _patchHeap.pop();
        if (patchFile->GetCurDocId() < docId) {
            auto st = patchFile->SkipCurDocValue<T>();
            RETURN2_IF_STATUS_ERROR(st, 0, "skip cur doc failed, docId[%d].", docId);
            st = PushBackToHeap(std::move(patchFile));
            RETURN2_IF_STATUS_ERROR(st, 0, "push patch file back to heap failed.");
            continue;
        } else if (patchFile->GetCurDocId() == docId) {
            bool isNull = false;
            auto [st, valueLen] = patchFile->GetPatchValue<T>(value, maxLen, isNull);
            RETURN2_IF_STATUS_ERROR(st, 0, "get patch value failed.");
            st = PushBackToHeap(std::move(patchFile));
            RETURN2_IF_STATUS_ERROR(st, 0, "push patch file back to heap failed.");
            st = SkipCurDocValue(docId);
            RETURN2_IF_STATUS_ERROR(st, 0, "skip cur doc failed, docId[%d].", docId);
            return std::make_pair(Status::OK(), valueLen);
        } else {
            _patchHeap.push(patchFile.release());
            return std::make_pair(Status::OK(), 0);
        }
    }
    return std::make_pair(Status::OK(), 0);
}

template <typename T>
inline std::pair<Status, size_t> MultiValueAttributePatchReader<T>::Next(docid_t& docId, uint8_t* value, size_t maxLen,
                                                                         bool& isNull)
{
    if (!HasNext()) {
        docId = INVALID_DOCID;
        return std::make_pair(Status::OK(), 0);
    }

    std::unique_ptr<MultiValueAttributePatchFile> patchFile(_patchHeap.top());
    docId = patchFile->GetCurDocId();
    _patchHeap.pop();
    auto [st, valueLen] = patchFile->GetPatchValue<T>(value, maxLen, isNull);
    RETURN2_IF_STATUS_ERROR(st, 0, "get patch value failed, docId[%d]", docId);
    st = PushBackToHeap(std::move(patchFile));
    RETURN2_IF_STATUS_ERROR(st, 0, "push patch file back to heap failed.");
    st = SkipCurDocValue(docId);
    RETURN2_IF_STATUS_ERROR(st, 0, "skip cur doc value failed, docId:%d.", docId);
    return std::make_pair(Status::OK(), valueLen);
}

template <typename T>
Status MultiValueAttributePatchReader<T>::SkipCurDocValue(docid_t docId)
{
    while (HasNext()) {
        std::unique_ptr<MultiValueAttributePatchFile> patchFile(_patchHeap.top());
        if (patchFile->GetCurDocId() != docId) {
            patchFile.release();
            return Status::OK();
        }
        _patchHeap.pop();
        auto st = patchFile->SkipCurDocValue<T>();
        RETURN_IF_STATUS_ERROR(st, "skip cur doc value failed.");
        st = PushBackToHeap(std::move(patchFile));
        RETURN_IF_STATUS_ERROR(st, "push patch file back to heap failed.");
    }
    return Status::OK();
}

template <typename T>
Status MultiValueAttributePatchReader<T>::PushBackToHeap(std::unique_ptr<MultiValueAttributePatchFile>&& patchFile)
{
    if (!patchFile->HasNext()) {
        patchFile.reset();
    } else {
        auto st = patchFile->Next();
        RETURN_IF_STATUS_ERROR(st, "patch file next operator failed.");
        _patchHeap.push(patchFile.release());
    }
    return Status::OK();
}
} // namespace indexlibv2::index
