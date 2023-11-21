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

#include "autil/MultiValueType.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/attribute_patch_reader.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_patch_file.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class VarNumAttributePatchComparator
{
public:
    bool operator()(VarNumAttributePatchFile*& lft, VarNumAttributePatchFile*& rht)
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
class VarNumAttributePatchReader : public AttributePatchReader
{
public:
    typedef std::priority_queue<VarNumAttributePatchFile*, std::vector<VarNumAttributePatchFile*>,
                                VarNumAttributePatchComparator>
        PatchHeap;

public:
    VarNumAttributePatchReader(const config::AttributeConfigPtr& attrConfig);
    virtual ~VarNumAttributePatchReader();

public:
    void AddPatchFile(const file_system::DirectoryPtr& directory, const std::string& fileName,
                      segmentid_t srcSegmentId) override;

    size_t Next(docid_t& docId, uint8_t* value, size_t maxLen, bool& isNull) override;

    bool HasNext() const override { return !mPatchHeap.empty(); }

    docid_t GetCurrentDocId() const override
    {
        assert(HasNext());
        VarNumAttributePatchFile* patchFile = mPatchHeap.top();
        return patchFile->GetCurDocId();
    }

    size_t Seek(docid_t docId, uint8_t* value, size_t maxLen) override;

    uint32_t GetMaxPatchItemLen() const override { return mMaxPatchValueLen; }

    size_t GetPatchFileLength() const override { return mPatchFlieLength; };

    size_t GetPatchItemCount() const override { return mPatchItemCount; }

protected:
    void SkipCurDocValue(docid_t docId);

private:
    void PushBackToHeap(std::unique_ptr<VarNumAttributePatchFile>&&);
    VarNumAttributePatchFile* CreatePatchFile(segmentid_t segmentId);

protected:
    PatchHeap mPatchHeap;
    uint32_t mMaxPatchValueLen;
    size_t mPatchFlieLength;
    size_t mPatchItemCount;

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP_TEMPLATE(index, VarNumAttributePatchReader);

/////////////////////////////////////////////////////
template <typename T>
VarNumAttributePatchReader<T>::VarNumAttributePatchReader(const config::AttributeConfigPtr& attrConfig)
    : AttributePatchReader(attrConfig)
    , mMaxPatchValueLen(0)
    , mPatchFlieLength(0)
    , mPatchItemCount(0)
{
}

template <typename T>
VarNumAttributePatchReader<T>::~VarNumAttributePatchReader()
{
    while (HasNext()) {
        VarNumAttributePatchFile* patchFile = mPatchHeap.top();
        mPatchHeap.pop();
        delete patchFile;
    }
}

template <typename T>
void VarNumAttributePatchReader<T>::AddPatchFile(const file_system::DirectoryPtr& directory,
                                                 const std::string& fileName, segmentid_t srcSegmentId)
{
    IE_LOG(DEBUG,
           "Add patch file(%s) in dir (%s)"
           " to patch reader.",
           fileName.c_str(), directory->DebugString().c_str());

    std::unique_ptr<VarNumAttributePatchFile> patchFile(CreatePatchFile(srcSegmentId));
    patchFile->Open(directory, fileName);
    mPatchFlieLength += patchFile->GetFileLength();
    mPatchItemCount += patchFile->GetPatchItemCount();
    if (patchFile->HasNext()) {
        patchFile->Next();
        mMaxPatchValueLen = std::max(mMaxPatchValueLen, patchFile->GetMaxPatchItemLen());
        mPatchHeap.push(patchFile.release());
    }
}

template <typename T>
VarNumAttributePatchFile* VarNumAttributePatchReader<T>::CreatePatchFile(segmentid_t segmentId)
{
    return new VarNumAttributePatchFile(segmentId, mAttrConfig);
}

template <typename T>
size_t VarNumAttributePatchReader<T>::Seek(docid_t docId, uint8_t* value, size_t maxLen)
{
    while (HasNext()) {
        std::unique_ptr<VarNumAttributePatchFile> patchFile(mPatchHeap.top());
        mPatchHeap.pop();
        if (patchFile->GetCurDocId() < docId) {
            patchFile->SkipCurDocValue<T>();
            PushBackToHeap(std::move(patchFile));
            continue;
        } else if (patchFile->GetCurDocId() == docId) {
            bool isNull = false;
            size_t valueLen = patchFile->GetPatchValue<T>(value, maxLen, isNull);
            PushBackToHeap(std::move(patchFile));
            SkipCurDocValue(docId);
            return valueLen;
        } else {
            mPatchHeap.push(patchFile.release());
            return 0;
        }
    }

    return 0;
}

template <typename T>
inline size_t VarNumAttributePatchReader<T>::Next(docid_t& docId, uint8_t* value, size_t maxLen, bool& isNull)
{
    if (!HasNext()) {
        docId = INVALID_DOCID;
        return 0;
    }

    std::unique_ptr<VarNumAttributePatchFile> patchFile(mPatchHeap.top());
    docId = patchFile->GetCurDocId();
    mPatchHeap.pop();
    size_t valueLen = patchFile->GetPatchValue<T>(value, maxLen, isNull);
    PushBackToHeap(std::move(patchFile));
    SkipCurDocValue(docId);
    return valueLen;
}

template <typename T>
void VarNumAttributePatchReader<T>::SkipCurDocValue(docid_t docId)
{
    while (HasNext()) {
        std::unique_ptr<VarNumAttributePatchFile> patchFile(mPatchHeap.top());
        if (patchFile->GetCurDocId() != docId) {
            patchFile.release();
            return;
        }
        mPatchHeap.pop();
        patchFile->SkipCurDocValue<T>();
        PushBackToHeap(std::move(patchFile));
    }
}

template <typename T>
void VarNumAttributePatchReader<T>::PushBackToHeap(std::unique_ptr<VarNumAttributePatchFile>&& patchFile)
{
    if (!patchFile->HasNext()) {
        patchFile.reset();
    } else {
        patchFile->Next();
        mPatchHeap.push(patchFile.release());
    }
}
}} // namespace indexlib::index
