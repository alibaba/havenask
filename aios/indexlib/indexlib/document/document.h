#ifndef __INDEXLIB_DOCUMENT_H
#define __INDEXLIB_DOCUMENT_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/document/locator.h"
#include <autil/DataBuffer.h>
#include <autil/StringUtil.h>

IE_NAMESPACE_BEGIN(document);

class Document
{
private:
    typedef std::map<std::string, std::string> TagInfoMap;
public:
    class TagIterator
    {
    public:
        TagIterator(const TagInfoMap& tagInfo)
            : mTagInfo(tagInfo)
        {
            mIter = mTagInfo.begin();
        }

        bool HasNext() const
        { return mIter != mTagInfo.end(); }

        void Next(std::string& tagKey, std::string& tagValue)
        {
            assert(HasNext());
            tagKey = mIter->first;
            tagValue = mIter->second;
            ++mIter;
        }
    private:
        const TagInfoMap& mTagInfo;
        TagInfoMap::const_iterator mIter;
    };
public:
    Document();
    Document(const Document &other);
    virtual ~Document();
public:
    bool operator==(const Document& doc) const;
    bool operator!=(const Document& doc) const {return !(*this == doc); }
    
    void SetSerializedVersion(uint32_t version);
    uint32_t GetSerializedVersion() const;
    
    const Locator& GetLocator() const { return mLocator; }
    void SetLocator(const Locator& locator) { mLocator = locator; }
    void SetLocator(const std::string& locatorStr)
    { mLocator.SetLocator(locatorStr); }
    
    void SetTimestamp(int64_t timestamp) { mTimestamp = timestamp; }
    int64_t GetTimestamp() const { return mTimestamp; }
    
    DocOperateType GetDocOperateType() const { return mOpType; }
    DocOperateType GetOriginalOperateType() const { return mOriginalOpType; }
    
    void SetTTL(uint32_t ttl) { mTTL = ttl; }
    uint32_t GetTTL() const { return mTTL; }

    void SetSource(const std::string &src) { AddTag(DOCUMENT_SOURCE_TAG_KEY, src); }
    const std::string& GetSource() const { return GetTag(DOCUMENT_SOURCE_TAG_KEY); }

    void AddTag(const std::string& key, const std::string& value);
    const std::string& GetTag(const std::string& key) const;

    TagIterator CreateTagIterator() const { return TagIterator(mTagInfo); }
    void SetIngestionTimestamp(int64_t ingestionTimestamp) { 
        AddTag(DOCUMENT_INGESTIONTIMESTAMP_TAG_KEY, std::to_string(ingestionTimestamp));
    }
    int64_t GetIngestionTimestamp() const { 
        const auto& ingestionTimestampStr = GetTag(DOCUMENT_INGESTIONTIMESTAMP_TAG_KEY);
        if (ingestionTimestampStr.empty()) {
            return INVALID_TIMESTAMP;
        }
        return autil::StringUtil::fromString<int64_t>(ingestionTimestampStr);
    }

public:
    void serialize(autil::DataBuffer &dataBuffer) const;
    void deserialize(autil::DataBuffer &dataBuffer);

public:
    virtual void SetDocOperateType(DocOperateType op) { mOpType = op; mOriginalOpType = op; }
    virtual void ModifyDocOperateType(DocOperateType op) { mOpType = op; }
    virtual bool HasFormatError() const { return false; }
    virtual uint32_t GetDocBinaryVersion() const = 0;
    virtual void SetDocId(docid_t docId) = 0;
    virtual docid_t GetDocId() const { return INVALID_DOCID; }
    
    //if no primary key, return empty string
    virtual const std::string &GetPrimaryKey() const;
    bool NeedTrace() const { return mTrace; }
    void SetTrace(bool trace) { mTrace = trace; }
    virtual size_t GetMessageCount() const { return 1u; }
    virtual size_t EstimateMemory() const { return sizeof(Document); }
protected:    
    virtual void DoSerialize(autil::DataBuffer &dataBuffer,
                             uint32_t serializedVersion) const = 0;

    virtual void DoDeserialize(autil::DataBuffer &dataBuffer,
                               uint32_t serializedVersion) = 0;

    std::string TagInfoToString() const;
    void TagInfoFromString(const std::string& str);
    
protected:
    DocOperateType mOpType;
    DocOperateType mOriginalOpType;
    mutable uint32_t mSerializedVersion;
    uint32_t mTTL;
    int64_t mTimestamp;
    TagInfoMap mTagInfo;  // legacy mSource
    Locator mLocator;
    bool mTrace; // used for doc trace
};

DEFINE_SHARED_PTR(Document);

IE_NAMESPACE_END(document);

#endif //__INDEXLIB_DOCUMENT_H
