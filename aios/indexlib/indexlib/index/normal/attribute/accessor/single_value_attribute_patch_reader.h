#ifndef __INDEXLIB_SINGLE_VALUE_ATTRIBUTE_PATCH_READER_H
#define __INDEXLIB_SINGLE_VALUE_ATTRIBUTE_PATCH_READER_H

#include <queue>
#include <memory>
#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/misc/exception.h"
#include "indexlib/index/normal/attribute/accessor/attribute_patch_reader.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/file_system/file_reader.h"
#include "indexlib/config/attribute_config.h"

IE_NAMESPACE_BEGIN(index);

class SinglePatchFile
{
public:
    //TODO: DELETE, legacy for merge
    void Open(const std::string& filePath);

public:
    SinglePatchFile(segmentid_t segmentId, bool patchCompress);
    ~SinglePatchFile();

    void Open(const file_system::DirectoryPtr& directory, 
              const std::string& fileName);

    bool HasNext();
    void Next();
    int64_t GetFileLength() const { return mFileLength; }
    template <typename T>
    T GetPatchValue();
    docid_t GetCurDocId() const { return mDocId; }
    segmentid_t GetSegmentId() const {return mSegmentId; }
private:
    int64_t mFileLength;
    int64_t mCursor;

    file_system::FileReaderPtr mFile;
    docid_t mDocId;
    segmentid_t mSegmentId;
    bool mPatchCompressed;

private:
    IE_LOG_DECLARE();
};

class PatchComparator
{
public:
    bool operator() (SinglePatchFile*& lft, SinglePatchFile*& rht)
    {
        if (lft->GetCurDocId() > rht->GetCurDocId())
        {
            return true;
        }
        else if (lft->GetCurDocId() < rht->GetCurDocId())
        {
            return false;
        }
        else
        {
            return (lft->GetSegmentId() > rht->GetSegmentId());
        }
    }
};

template <typename T>
class SingleValueAttributePatchReader : public AttributePatchReader
{
public:
    typedef std::priority_queue<SinglePatchFile*, 
                                std::vector<SinglePatchFile*>, 
                                PatchComparator> PatchHeap;

public:
    SingleValueAttributePatchReader(const config::AttributeConfigPtr& attrConfig);
    virtual ~SingleValueAttributePatchReader();

public:
    //TODO: DELETE
    void AddPatchFile(
            const std::string& filePath, segmentid_t segmentId);
public:
    
    void AddPatchFile(
            const file_system::DirectoryPtr& directory,
            const std::string& fileName, segmentid_t srcSegmentId) override;

    size_t Next(
            docid_t& docId, uint8_t* buffer, size_t bufferLen) override;

    size_t Seek(
            docid_t docId, uint8_t* value, size_t maxLen) override;

    docid_t GetCurrentDocId() const override
    {
        assert(HasNext());
        SinglePatchFile* patchFile = mPatchHeap.top();
        return patchFile->GetCurDocId();
    }

    bool HasNext() const override
    { return !mPatchHeap.empty(); }

    uint32_t GetMaxPatchItemLen() const override
    { return sizeof(T); }
    size_t GetPatchFileLength() const override
    { return mPatchFlieLength; };
    size_t GetPatchItemCount() const override
    { return mPatchItemCount; }

    bool ReadPatch(docid_t docId, T& value);
    bool ReadPatch(docid_t docId, uint8_t* value, size_t size);
protected:
    inline bool InnerReadPatch(docid_t docId, T& value)  __attribute__((always_inline));

private:
    inline void PushBackToHeap(std::unique_ptr<SinglePatchFile>&&)  __attribute__((always_inline));

protected:
    PatchHeap mPatchHeap;
    size_t mPatchFlieLength;
    size_t mPatchItemCount;

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP_TEMPLATE(index, SingleValueAttributePatchReader);

///////////////////////////////////////////////////////

inline bool SinglePatchFile::HasNext()
{
    return mCursor < mFileLength;
}

inline void SinglePatchFile::Next()
{
    if (mFile->Read((void*)(&mDocId), sizeof(docid_t), 
                    mCursor) < (ssize_t)sizeof(docid_t))
    {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, 
                             "Read patch file failed, file path is: %s", 
                             mFile->GetPath().c_str());
    }
    mCursor += sizeof(docid_t);
}

template <typename T>
inline T SinglePatchFile::GetPatchValue()
{
    T value;
    if (mFile->Read((void*)(&value), sizeof(T), 
                    mCursor) < (ssize_t)sizeof(T))
    {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, 
                             "Read patch file failed, file path is: %s", 
                             mFile->GetPath().c_str());
    }
    mCursor += sizeof(T);
    return value;
}

/////////////////////////////////////////////////////
template <typename T>
SingleValueAttributePatchReader<T>::SingleValueAttributePatchReader(
        const config::AttributeConfigPtr& attrConfig)
    : AttributePatchReader(attrConfig)
    , mPatchFlieLength(0)
    , mPatchItemCount(0)
{
}

template <typename T>
SingleValueAttributePatchReader<T>::~SingleValueAttributePatchReader()
{
    while (HasNext())
    {
        SinglePatchFile* patchFile = mPatchHeap.top();
        mPatchHeap.pop();
        delete patchFile;
    }
}

template <typename T>
size_t SingleValueAttributePatchReader<T>::Next(
        docid_t& docId, uint8_t* buffer, size_t bufferLen)
{
    if (unlikely(bufferLen < sizeof(T)))
    {
        INDEXLIB_FATAL_ERROR(BufferOverflow, "buffer is not big enough!");
    }

    if (!this->HasNext())
    {
        return 0;
    }

    std::unique_ptr<SinglePatchFile> patchFile(this->mPatchHeap.top());
    docId = patchFile->GetCurDocId();
    
    T& value = *(T*)buffer;
    if (this->InnerReadPatch(docId, value))
    {
        patchFile.release();
        return sizeof(T);
    }
    patchFile.release();
    return 0;
}

template <typename T>
size_t SingleValueAttributePatchReader<T>::Seek(
        docid_t docId, uint8_t* value, size_t maxLen)
{
    if (unlikely(maxLen < sizeof(T)))
    {
        INDEXLIB_FATAL_ERROR(BufferOverflow, "buffer is not big enough!");
    }

    T& data = *(T*)value;
    return ReadPatch(docId, data) ? sizeof(T) : 0;
}

template <typename T>
void SingleValueAttributePatchReader<T>::AddPatchFile(
        const file_system::DirectoryPtr& directory,
        const std::string& fileName, segmentid_t srcSegmentId)
{
    IE_LOG(INFO, "Add patch file(%s) in dir (%s)"
           " to patch reader.", fileName.c_str(),
           directory->GetPath().c_str());

    std::unique_ptr<SinglePatchFile> patchFile(new SinglePatchFile(srcSegmentId,
                    mAttrConfig->GetCompressType().HasPatchCompress()));
    patchFile->Open(directory, fileName);
    mPatchFlieLength += patchFile->GetFileLength();
    mPatchItemCount += (patchFile->GetFileLength() / (sizeof(docid_t) + sizeof(T)));
    if (patchFile->HasNext())
    {
        patchFile->Next();
        mPatchHeap.push(patchFile.release());
    }
}

template <typename T>
void SingleValueAttributePatchReader<T>::AddPatchFile(
        const std::string& filePath, segmentid_t segmentId)
{
    IE_LOG(INFO, "Add patch file(%s) to patch reader.", filePath.c_str());
    std::unique_ptr<SinglePatchFile> patchFile(new SinglePatchFile(segmentId,
                    mAttrConfig->GetCompressType().HasPatchCompress()));
    patchFile->Open(filePath);
    mPatchFlieLength += patchFile->GetFileLength();
    mPatchItemCount += (patchFile->GetFileLength() / (sizeof(docid_t) + sizeof(T)));    
    if (patchFile->HasNext())
    {
        patchFile->Next();
        mPatchHeap.push(patchFile.release());
    }
}

template <typename T>
bool SingleValueAttributePatchReader<T>::ReadPatch(
        docid_t docId, uint8_t* value, size_t size)
{
    assert(size == sizeof(T));
    return ReadPatch(docId, *(T*)value);
}

template <typename T>
bool SingleValueAttributePatchReader<T>::ReadPatch(docid_t docId, T& value)
{
    if (!HasNext())
    {
        return false;
    }

    SinglePatchFile* patchFile = mPatchHeap.top();
    if (patchFile->GetCurDocId() > docId)
    {
        return false;
    }
    return InnerReadPatch(docId, value);
}

template <typename T>
bool SingleValueAttributePatchReader<T>::InnerReadPatch(
        docid_t docId, T& value)
{
    assert(HasNext());
    bool isFind = false;
    while (HasNext())
    {
        std::unique_ptr<SinglePatchFile> patchFile(mPatchHeap.top());
        if (patchFile->GetCurDocId() >= docId)
        {
            if (patchFile->GetCurDocId() == docId)
            {
                isFind = true;
            }
            patchFile.release();
            break;
        }
        mPatchHeap.pop();
        value = patchFile->GetPatchValue<T>();
        PushBackToHeap(std::move(patchFile));
    }
    
    if (!isFind)
    {
        return false;
    }

    while (HasNext())
    {
        std::unique_ptr<SinglePatchFile> patchFile(mPatchHeap.top());
        if (patchFile->GetCurDocId() != docId)
        {
            patchFile.release();
            break;
        }
        mPatchHeap.pop();
        value = patchFile->GetPatchValue<T>();
        PushBackToHeap(std::move(patchFile));
    }

    return true;
}

template <typename T>
void SingleValueAttributePatchReader<T>::PushBackToHeap(
        std::unique_ptr<SinglePatchFile>&& patchFile)
{
    if (!patchFile->HasNext())
    {
        patchFile.reset();
    }
    else
    {
        patchFile->Next();
        mPatchHeap.push(patchFile.release());
    }
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_SINGLE_VALUE_ATTRIBUTE_PATCH_READER_H

