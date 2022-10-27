#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/misc/exception.h"
#include <fstream>

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(document);
IE_LOG_SETUP(document, NormalDocument);

NormalDocument::NormalDocument(PoolPtr pool)
    : mUserTimestamp(INVALID_TIMESTAMP)
    , mRegionId(DEFAULT_REGIONID)
    , mSchemaId(DEFAULT_SCHEMAID)
    , mPool(pool)
    , mSegmentIdBeforeModified(INVALID_SEGMENTID)
{
    assert(mPool);
}

NormalDocument::~NormalDocument()
{
    mIndexDocument.reset();
    mAttributeDocument.reset();
    mSummaryDocument.reset();

    mPool.reset();
}

bool NormalDocument::operator==(const NormalDocument& doc) const
{
    if (Document::operator!=(doc))
    {
        return false;
    }

    // index document
    if (mIndexDocument)
    {
        if (doc.mIndexDocument == NULL
            || *mIndexDocument != *(doc.mIndexDocument))
        {
            return false;
        }
    }
    else if (doc.mIndexDocument != NULL)
    {
        return false;
    }

    // attribute document
    if (mAttributeDocument)
    {
        if (doc.mAttributeDocument == NULL
            || *mAttributeDocument != *(doc.mAttributeDocument))
        {
            return false;
        }
    }
    else if (doc.mAttributeDocument != NULL)
    {
        return false;
    }

    // summary document
    if (mSummaryDocument)
    {
        if (doc.mSummaryDocument == NULL
            || *mSummaryDocument != *(doc.mSummaryDocument))
        {
            return false;
        }
    }
    else if (doc.mSummaryDocument != NULL)
    {
        return false;
    }

    if (mKVIndexDocument)
    {
        if (doc.mKVIndexDocument == NULL
            || *mKVIndexDocument != *(doc.mKVIndexDocument))
        {
            return false;
        }
    }
    else if (doc.mKVIndexDocument != NULL)
    {
        return false;
    }

    if(mSubDocuments.size() != doc.mSubDocuments.size())
    {
        return false;
    }
    else
    {
        for(size_t i = 0; i < mSubDocuments.size(); ++i)
        {
            if(mSubDocuments[i])
            {
                if(!doc.mSubDocuments[i]) { return false; }
                if(*mSubDocuments[i] != *doc.mSubDocuments[i]) { return false; }
            }
            else
            {
                if(doc.mSubDocuments[i]) { return false; }
            }
        }
    }
    
    if(mModifiedFields.size() != doc.mModifiedFields.size())
    {
        return false;
    }
    else
    {
        for(size_t i = 0; i< mModifiedFields.size(); ++i)
        {
            if(mModifiedFields[i] != doc.mModifiedFields[i]) { return false; }
        }
    }

    if(mSubModifiedFields.size() != doc.mSubModifiedFields.size())
    {
        return false;
    }
    else
    {
        for(size_t i = 0; i< mSubModifiedFields.size(); ++i)
        {
            if(mSubModifiedFields[i] != doc.mSubModifiedFields[i]) { return false; }
        }
    }
    
    if (mUserTimestamp != doc.mUserTimestamp) { return false; }
    if (mSegmentIdBeforeModified != doc.mSegmentIdBeforeModified) { return false; }
    if (mRegionId != doc.mRegionId) { return false; }
    if (mSchemaId != doc.mSchemaId) { return false; }

    return true;
}

void NormalDocument::DoSerialize(DataBuffer &dataBuffer, uint32_t serializedVersion) const
{
    if (serializedVersion != DOCUMENT_BINARY_VERSION)
    {
        for (size_t i = 0; i < mSubDocuments.size(); ++i)
        {
            mSubDocuments[i]->SetSerializedVersion(serializedVersion);
        }
    }
    
    switch (serializedVersion)
    {
    case DOCUMENT_BINARY_VERSION:
        serializeVersion7(dataBuffer);
        break;
    case 6:
        serializeVersion6(dataBuffer);
        break;
    case 5:
        serializeVersion5(dataBuffer);
        break;
    case 4:
        serializeVersion4(dataBuffer);
        break;
    case 3:
        serializeVersion3(dataBuffer);
        break;
        
    default:
        INDEXLIB_THROW(misc::BadParameterException,
                       "document serialize failed, "
                       "do not support serialize document with version %u",
                       serializedVersion);
    }
}

void NormalDocument::serializeVersion3(DataBuffer &dataBuffer) const
{
    dataBuffer.write(mIndexDocument);
    dataBuffer.write(mAttributeDocument);
    dataBuffer.write(mSummaryDocument);
    dataBuffer.write(mSubDocuments);
    dataBuffer.write(mModifiedFields);
    dataBuffer.write(mSubModifiedFields);
    dataBuffer.write(mTimestamp);

    string source = TagInfoToString();
    dataBuffer.write(source);
    dataBuffer.write(mLocator);
    dataBuffer.write(mOpType);
    dataBuffer.write(mOriginalOpType);
}

void NormalDocument::serializeVersion4(DataBuffer &dataBuffer) const
{
    // add kv index document
    dataBuffer.write(mIndexDocument);
    dataBuffer.write(mAttributeDocument);
    dataBuffer.write(mSummaryDocument);
    dataBuffer.write(mSubDocuments);
    dataBuffer.write(mModifiedFields);
    dataBuffer.write(mSubModifiedFields);
    dataBuffer.write(mTimestamp);

    string source = TagInfoToString();
    dataBuffer.write(source);
    dataBuffer.write(mLocator);
    dataBuffer.write(mOpType);
    dataBuffer.write(mOriginalOpType);
    dataBuffer.write(mKVIndexDocument);
}

void NormalDocument::serializeVersion5(DataBuffer &dataBuffer) const
{
    // add user timestamp
    dataBuffer.write(mIndexDocument);
    dataBuffer.write(mAttributeDocument);
    dataBuffer.write(mSummaryDocument);
    dataBuffer.write(mSubDocuments);
    dataBuffer.write(mModifiedFields);
    dataBuffer.write(mSubModifiedFields);
    dataBuffer.write(mTimestamp);
    bool isValidUserTS = mUserTimestamp != INVALID_TIMESTAMP;
    dataBuffer.write(isValidUserTS);
    if (isValidUserTS)
    {
        dataBuffer.write(mUserTimestamp);
    }

    string source = TagInfoToString();
    dataBuffer.write(source);
    dataBuffer.write(mLocator);
    dataBuffer.write(mOpType);
    dataBuffer.write(mOriginalOpType);
    dataBuffer.write(mKVIndexDocument);
}

void NormalDocument::serializeVersion6(DataBuffer &dataBuffer) const
{
    // add regionId
    dataBuffer.write(mIndexDocument);
    dataBuffer.write(mAttributeDocument);
    dataBuffer.write(mSummaryDocument);
    dataBuffer.write(mSubDocuments);
    dataBuffer.write(mModifiedFields);
    dataBuffer.write(mSubModifiedFields);
    dataBuffer.write(mTimestamp);
    bool isValidUserTS = mUserTimestamp != INVALID_TIMESTAMP;
    dataBuffer.write(isValidUserTS);
    if (isValidUserTS)
    {
        dataBuffer.write(mUserTimestamp);
    }
    dataBuffer.write(mRegionId);

    string source = TagInfoToString();
    dataBuffer.write(source);
    dataBuffer.write(mLocator);
    dataBuffer.write(mOpType);
    dataBuffer.write(mOriginalOpType);
    dataBuffer.write(mKVIndexDocument);
}

void NormalDocument::serializeVersion7(DataBuffer &dataBuffer) const
{
    // add schemaId & trace
    dataBuffer.write(mIndexDocument);
    dataBuffer.write(mAttributeDocument);
    dataBuffer.write(mSummaryDocument);
    dataBuffer.write(mSubDocuments);
    dataBuffer.write(mModifiedFields);
    dataBuffer.write(mSubModifiedFields);
    dataBuffer.write(mTimestamp);
    bool isValidUserTS = mUserTimestamp != INVALID_TIMESTAMP;
    dataBuffer.write(isValidUserTS);
    if (isValidUserTS)
    {
        dataBuffer.write(mUserTimestamp);
    }
    dataBuffer.write(mRegionId);

    string source = TagInfoToString();
    dataBuffer.write(source);
    dataBuffer.write(mLocator);
    dataBuffer.write(mOpType);
    dataBuffer.write(mOriginalOpType);
    dataBuffer.write(mKVIndexDocument);
    
    dataBuffer.write(mSchemaId);
    dataBuffer.write(mTrace);
}

void NormalDocument::deserializeVersion3(DataBuffer &dataBuffer)
{
    assert(mPool);
    uint32_t documentVersion = 3;
    mIndexDocument.reset(deserializeObject<IndexDocument>(
                    dataBuffer, mPool.get(), documentVersion));
    mAttributeDocument.reset(deserializeObject<AttributeDocument>(
                    dataBuffer, mPool.get(), documentVersion));
    mSummaryDocument.reset(deserializeObject<SerializedSummaryDocument>(
                    dataBuffer, documentVersion));

    dataBuffer.read(mSubDocuments);
    dataBuffer.read(mModifiedFields);
    dataBuffer.read(mSubModifiedFields);
    dataBuffer.read(mTimestamp);

    string source;
    dataBuffer.read(source);
    TagInfoFromString(source);
    dataBuffer.read(mLocator, mPool.get());
    dataBuffer.read(mOpType);
    dataBuffer.read(mOriginalOpType);
}

void NormalDocument::deserializeVersion4(DataBuffer &dataBuffer)
{
    assert(mPool);
    uint32_t documentVersion = 4;
    mIndexDocument.reset(deserializeObject<IndexDocument>(
                    dataBuffer, mPool.get(), documentVersion));
    mAttributeDocument.reset(deserializeObject<AttributeDocument>(
                    dataBuffer, mPool.get(), documentVersion));
    mSummaryDocument.reset(deserializeObject<SerializedSummaryDocument>(
                    dataBuffer, documentVersion));

    dataBuffer.read(mSubDocuments);
    dataBuffer.read(mModifiedFields);
    dataBuffer.read(mSubModifiedFields);
    dataBuffer.read(mTimestamp);
    
    string source;
    dataBuffer.read(source);
    TagInfoFromString(source);

    dataBuffer.read(mLocator, mPool.get());

    dataBuffer.read(mOpType);
    dataBuffer.read(mOriginalOpType);
    mKVIndexDocument.reset(deserializeObject<KVIndexDocument>(
                    dataBuffer, mPool.get(), documentVersion));
}

void NormalDocument::deserializeVersion5(DataBuffer &dataBuffer)
{
    assert(mPool);
    uint32_t documentVersion = 5;
    mIndexDocument.reset(deserializeObject<IndexDocument>(
                    dataBuffer, mPool.get(), documentVersion));
    mAttributeDocument.reset(deserializeObject<AttributeDocument>(
                    dataBuffer, mPool.get(), documentVersion));
    mSummaryDocument.reset(deserializeObject<SerializedSummaryDocument>(
                    dataBuffer, documentVersion));

    dataBuffer.read(mSubDocuments);
    dataBuffer.read(mModifiedFields);
    dataBuffer.read(mSubModifiedFields);
    dataBuffer.read(mTimestamp);

    bool isValidUserTS;
    dataBuffer.read(isValidUserTS);
    if (isValidUserTS)
    {
        dataBuffer.read(mUserTimestamp);
    }

    string source;
    dataBuffer.read(source);
    TagInfoFromString(source);

    dataBuffer.read(mLocator, mPool.get());
    dataBuffer.read(mOpType);
    dataBuffer.read(mOriginalOpType);
    mKVIndexDocument.reset(deserializeObject<KVIndexDocument>(
                    dataBuffer, mPool.get(), documentVersion));
}

void NormalDocument::deserializeVersion6(DataBuffer &dataBuffer)
{
    assert(mPool);
    uint32_t documentVersion = 6;
    mIndexDocument.reset(deserializeObject<IndexDocument>(
                    dataBuffer, mPool.get(), documentVersion));
    mAttributeDocument.reset(deserializeObject<AttributeDocument>(
                    dataBuffer, mPool.get(), documentVersion));
    mSummaryDocument.reset(deserializeObject<SerializedSummaryDocument>(
                    dataBuffer, documentVersion));

    dataBuffer.read(mSubDocuments);
    dataBuffer.read(mModifiedFields);
    dataBuffer.read(mSubModifiedFields);
    dataBuffer.read(mTimestamp);

    bool isValidUserTS;
    dataBuffer.read(isValidUserTS);
    if (isValidUserTS)
    {
        dataBuffer.read(mUserTimestamp);
    }

    dataBuffer.read(mRegionId);

    string source;
    dataBuffer.read(source);
    TagInfoFromString(source);

    dataBuffer.read(mLocator, mPool.get());
    dataBuffer.read(mOpType);
    dataBuffer.read(mOriginalOpType);
    mKVIndexDocument.reset(deserializeObject<KVIndexDocument>(
                    dataBuffer, mPool.get(), documentVersion));
}

void NormalDocument::deserializeVersion7(DataBuffer &dataBuffer)
{
    assert(mPool);
    uint32_t documentVersion = 7;
    mIndexDocument.reset(deserializeObject<IndexDocument>(
                    dataBuffer, mPool.get(), documentVersion));
    mAttributeDocument.reset(deserializeObject<AttributeDocument>(
                    dataBuffer, mPool.get(), documentVersion));
    mSummaryDocument.reset(deserializeObject<SerializedSummaryDocument>(
                    dataBuffer, documentVersion));

    dataBuffer.read(mSubDocuments);
    dataBuffer.read(mModifiedFields);
    dataBuffer.read(mSubModifiedFields);
    dataBuffer.read(mTimestamp);

    bool isValidUserTS;
    dataBuffer.read(isValidUserTS);
    if (isValidUserTS)
    {
        dataBuffer.read(mUserTimestamp);
    }

    dataBuffer.read(mRegionId);

    string source;
    dataBuffer.read(source);
    TagInfoFromString(source);

    dataBuffer.read(mLocator, mPool.get());
    dataBuffer.read(mOpType);
    dataBuffer.read(mOriginalOpType);
    mKVIndexDocument.reset(deserializeObject<KVIndexDocument>(
                               dataBuffer, mPool.get(), documentVersion));
    
    dataBuffer.read(mSchemaId);
    dataBuffer.read(mTrace);
}

void NormalDocument::DoDeserialize(DataBuffer &dataBuffer,
                                   uint32_t serializedVersion)
{
    assert(mPool);
    // since version 7, all higher version can be deserialize by version7
    // fields in different version were stored in different zone
    // -----v7----|---v8--
    // [f1, f2, f3][f4, f5]
    if (serializedVersion >= 7 && DOCUMENT_BINARY_VERSION == 7) {
        deserializeVersion7(dataBuffer);
        return;
    }
    switch (serializedVersion)
    {
    case DOCUMENT_BINARY_VERSION:
        deserializeVersion7(dataBuffer);
        break;
    case 6:
        deserializeVersion6(dataBuffer);
        break;
    case 5:
        deserializeVersion5(dataBuffer);
        break;
    case 4:
        deserializeVersion4(dataBuffer);
        break;
    case 3:
        deserializeVersion3(dataBuffer);
        break;
        
    default:
        std::stringstream ss;
        ss << "document deserialize failed, expect version: "
           << DOCUMENT_BINARY_VERSION
           << " actual version: "
           << serializedVersion;
        INDEXLIB_THROW(misc::DocumentDeserializeException, "%s", ss.str().c_str());
    }
}

string NormalDocument::SerializeToString() const {
    DataBuffer dataBuffer;
    dataBuffer.write(*this);
    return string(dataBuffer.getData(), dataBuffer.getDataLen());
}

void NormalDocument::DeserializeFromString(const string &docStr) {
    DataBuffer dataBuffer(const_cast<char*>(docStr.data()), docStr.length());
    dataBuffer.read(*this);
}

void NormalDocument::ModifyDocOperateType(DocOperateType op)
{ 
    mOpType = op; 
    for (size_t i = 0; i < mSubDocuments.size(); ++i)
    {
        mSubDocuments[i]->ModifyDocOperateType(op);
    }
}

const std::string& NormalDocument::GetPrimaryKey() const
{
    if (mIndexDocument)
    {
        return mIndexDocument->GetPrimaryKey();
    }
    return Document::GetPrimaryKey();
}

size_t NormalDocument::GetMemoryUse() const
{
    size_t memUse = mPool->getUsedBytes();
    for (size_t i = 0; i < mSubDocuments.size(); ++i)
    {
        memUse += mSubDocuments[i]->GetMemoryUse();
    }
    return memUse;
}

size_t NormalDocument::EstimateMemory() const
{
    size_t ret = sizeof(*this);
    ret += mPool->getTotalBytes();
    for (size_t i = 0; i < mSubDocuments.size(); ++i)
    {
        ret += mSubDocuments[i]->EstimateMemory();
    }
    return ret;
}

bool NormalDocument::HasFormatError() const
{
    for (size_t i = 0; i < mSubDocuments.size(); i++)
    {
        const NormalDocumentPtr& subDoc = mSubDocuments[i];
        AttributeDocumentPtr subAttrDoc;
        if (subDoc)
        {
            subAttrDoc = subDoc->GetAttributeDocument();
        }
        if (subAttrDoc && subAttrDoc->HasFormatError())
        {
            return true;
        }
    }
    return mAttributeDocument && mAttributeDocument->HasFormatError();
}

IE_NAMESPACE_END(document);
