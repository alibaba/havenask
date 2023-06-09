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
#include "indexlib/document/index_document/normal_document/normal_document.h"

#include <fstream>

#include "indexlib/document/normal/NormalDocument.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

namespace indexlib { namespace document {
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
    if (Document::operator!=(doc)) {
        return false;
    }

    // index document
    if (mIndexDocument) {
        if (doc.mIndexDocument == NULL || *mIndexDocument != *(doc.mIndexDocument)) {
            return false;
        }
    } else if (doc.mIndexDocument != NULL) {
        return false;
    }

    // attribute document
    if (mAttributeDocument) {
        if (doc.mAttributeDocument == NULL || *mAttributeDocument != *(doc.mAttributeDocument)) {
            return false;
        }
    } else if (doc.mAttributeDocument != NULL) {
        return false;
    }

    // summary document
    if (mSummaryDocument) {
        if (doc.mSummaryDocument == NULL || *mSummaryDocument != *(doc.mSummaryDocument)) {
            return false;
        }
    } else if (doc.mSummaryDocument != NULL) {
        return false;
    }

    if (mSubDocuments.size() != doc.mSubDocuments.size()) {
        return false;
    } else {
        for (size_t i = 0; i < mSubDocuments.size(); ++i) {
            if (mSubDocuments[i]) {
                if (!doc.mSubDocuments[i]) {
                    return false;
                }
                if (*mSubDocuments[i] != *doc.mSubDocuments[i]) {
                    return false;
                }
            } else {
                if (doc.mSubDocuments[i]) {
                    return false;
                }
            }
        }
    }

    if (mModifiedFields.size() != doc.mModifiedFields.size()) {
        return false;
    } else {
        for (size_t i = 0; i < mModifiedFields.size(); ++i) {
            if (mModifiedFields[i] != doc.mModifiedFields[i]) {
                return false;
            }
        }
    }

    if (mSubModifiedFields.size() != doc.mSubModifiedFields.size()) {
        return false;
    } else {
        for (size_t i = 0; i < mSubModifiedFields.size(); ++i) {
            if (mSubModifiedFields[i] != doc.mSubModifiedFields[i]) {
                return false;
            }
        }
    }

    if (mUserTimestamp != doc.mUserTimestamp) {
        return false;
    }
    if (mSegmentIdBeforeModified != doc.mSegmentIdBeforeModified) {
        return false;
    }
    if (mRegionId != doc.mRegionId) {
        return false;
    }
    if (mSchemaId != doc.mSchemaId) {
        return false;
    }

    return true;
}

void NormalDocument::DoSerialize(DataBuffer& dataBuffer, uint32_t serializedVersion) const
{
    if (serializedVersion != LEGACY_DOCUMENT_BINARY_VERSION) {
        for (size_t i = 0; i < mSubDocuments.size(); ++i) {
            mSubDocuments[i]->SetSerializedVersion(serializedVersion);
        }
    }

    switch (serializedVersion) {
    case LEGACY_DOCUMENT_BINARY_VERSION:
        serializeVersion7(dataBuffer);
        serializeVersion8(dataBuffer);
        serializeVersion9(dataBuffer);
        serializeVersion10(dataBuffer);
        break;
    case 9:
        serializeVersion7(dataBuffer);
        serializeVersion8(dataBuffer);
        serializeVersion9(dataBuffer);
        break;
    case 8:
        serializeVersion7(dataBuffer);
        serializeVersion8(dataBuffer);
        break;
    case 7:
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
        INDEXLIB_THROW(util::BadParameterException,
                       "document serialize failed, "
                       "do not support serialize document with version %u",
                       serializedVersion);
    }
}

void NormalDocument::serializeVersion3(DataBuffer& dataBuffer) const
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

void NormalDocument::serializeVersion4(DataBuffer& dataBuffer) const
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

void NormalDocument::serializeVersion5(DataBuffer& dataBuffer) const
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
    if (isValidUserTS) {
        dataBuffer.write(mUserTimestamp);
    }

    string source = TagInfoToString();
    dataBuffer.write(source);
    dataBuffer.write(mLocator);
    dataBuffer.write(mOpType);
    dataBuffer.write(mOriginalOpType);
    dataBuffer.write(mKVIndexDocument);
}

void NormalDocument::serializeVersion6(DataBuffer& dataBuffer) const
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
    if (isValidUserTS) {
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

void NormalDocument::serializeVersion7(DataBuffer& dataBuffer) const
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
    if (isValidUserTS) {
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

void NormalDocument::deserializeVersion3(DataBuffer& dataBuffer)
{
    assert(mPool);
    uint32_t documentVersion = 3;
    mIndexDocument.reset(deserializeObject<IndexDocument>(dataBuffer, mPool.get(), documentVersion));
    mAttributeDocument.reset(deserializeObject<AttributeDocument>(dataBuffer, mPool.get(), documentVersion));
    mSummaryDocument.reset(deserializeObject<SerializedSummaryDocument>(dataBuffer, documentVersion));

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

void NormalDocument::deserializeVersion4(DataBuffer& dataBuffer)
{
    assert(mPool);
    uint32_t documentVersion = 4;
    mIndexDocument.reset(deserializeObject<IndexDocument>(dataBuffer, mPool.get(), documentVersion));
    mAttributeDocument.reset(deserializeObject<AttributeDocument>(dataBuffer, mPool.get(), documentVersion));
    mSummaryDocument.reset(deserializeObject<SerializedSummaryDocument>(dataBuffer, documentVersion));

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
    mKVIndexDocument.reset(deserializeObject<legacy::KVIndexDocument>(dataBuffer, mPool.get(), documentVersion));
}

void NormalDocument::deserializeVersion5(DataBuffer& dataBuffer)
{
    assert(mPool);
    uint32_t documentVersion = 5;
    mIndexDocument.reset(deserializeObject<IndexDocument>(dataBuffer, mPool.get(), documentVersion));
    mAttributeDocument.reset(deserializeObject<AttributeDocument>(dataBuffer, mPool.get(), documentVersion));
    mSummaryDocument.reset(deserializeObject<SerializedSummaryDocument>(dataBuffer, documentVersion));

    dataBuffer.read(mSubDocuments);
    dataBuffer.read(mModifiedFields);
    dataBuffer.read(mSubModifiedFields);
    dataBuffer.read(mTimestamp);

    bool isValidUserTS;
    dataBuffer.read(isValidUserTS);
    if (isValidUserTS) {
        dataBuffer.read(mUserTimestamp);
    }

    string source;
    dataBuffer.read(source);
    TagInfoFromString(source);

    dataBuffer.read(mLocator, mPool.get());
    dataBuffer.read(mOpType);
    dataBuffer.read(mOriginalOpType);
    mKVIndexDocument.reset(deserializeObject<legacy::KVIndexDocument>(dataBuffer, mPool.get(), documentVersion));
}

void NormalDocument::deserializeVersion6(DataBuffer& dataBuffer)
{
    assert(mPool);
    uint32_t documentVersion = 6;
    mIndexDocument.reset(deserializeObject<IndexDocument>(dataBuffer, mPool.get(), documentVersion));
    mAttributeDocument.reset(deserializeObject<AttributeDocument>(dataBuffer, mPool.get(), documentVersion));
    mSummaryDocument.reset(deserializeObject<SerializedSummaryDocument>(dataBuffer, documentVersion));

    dataBuffer.read(mSubDocuments);
    dataBuffer.read(mModifiedFields);
    dataBuffer.read(mSubModifiedFields);
    dataBuffer.read(mTimestamp);

    bool isValidUserTS;
    dataBuffer.read(isValidUserTS);
    if (isValidUserTS) {
        dataBuffer.read(mUserTimestamp);
    }

    dataBuffer.read(mRegionId);

    string source;
    dataBuffer.read(source);
    TagInfoFromString(source);

    dataBuffer.read(mLocator, mPool.get());
    dataBuffer.read(mOpType);
    dataBuffer.read(mOriginalOpType);
    mKVIndexDocument.reset(deserializeObject<legacy::KVIndexDocument>(dataBuffer, mPool.get(), documentVersion));
}

void NormalDocument::deserializeVersion7(DataBuffer& dataBuffer)
{
    assert(mPool);
    uint32_t documentVersion = 7;
    mIndexDocument.reset(deserializeObject<IndexDocument>(dataBuffer, mPool.get(), documentVersion));
    mAttributeDocument.reset(deserializeObject<AttributeDocument>(dataBuffer, mPool.get(), documentVersion));
    mSummaryDocument.reset(deserializeObject<SerializedSummaryDocument>(dataBuffer, documentVersion));

    dataBuffer.read(mSubDocuments);
    dataBuffer.read(mModifiedFields);
    dataBuffer.read(mSubModifiedFields);
    dataBuffer.read(mTimestamp);

    bool isValidUserTS;
    dataBuffer.read(isValidUserTS);
    if (isValidUserTS) {
        dataBuffer.read(mUserTimestamp);
    }

    dataBuffer.read(mRegionId);

    string source;
    dataBuffer.read(source);
    TagInfoFromString(source);

    dataBuffer.read(mLocator, mPool.get());
    dataBuffer.read(mOpType);
    dataBuffer.read(mOriginalOpType);
    mKVIndexDocument.reset(deserializeObject<legacy::KVIndexDocument>(dataBuffer, mPool.get(), documentVersion));

    dataBuffer.read(mSchemaId);
    dataBuffer.read(mTrace);
}

void NormalDocument::DoDeserialize(DataBuffer& dataBuffer, uint32_t serializedVersion)
{
    assert(mPool);
    // since version 7, all higher version can be deserialize by version7
    // fields in different version were stored in different zone
    // -----v7-----|---v8--|----v9---|
    // [f1, f2, f3][f4, f5 ][ f6, f7 ]
    if (serializedVersion >= LEGACY_DOCUMENT_BINARY_VERSION) {
        deserializeVersion7(dataBuffer);
        deserializeVersion8(dataBuffer);
        deserializeVersion9(dataBuffer);
        deserializeVersion10(dataBuffer);
        return;
    }
    switch (serializedVersion) {
    case 9:
        deserializeVersion7(dataBuffer);
        deserializeVersion8(dataBuffer);
        deserializeVersion9(dataBuffer);
        break;
    case 8:
        deserializeVersion7(dataBuffer);
        deserializeVersion8(dataBuffer);
        break;
    case 7:
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
        ss << "document deserialize failed, expect version: " << LEGACY_DOCUMENT_BINARY_VERSION
           << " actual version: " << serializedVersion;
        INDEXLIB_THROW(util::DocumentDeserializeException, "%s", ss.str().c_str());
    }
}

string NormalDocument::SerializeToString() const
{
    DataBuffer dataBuffer;
    dataBuffer.write(*this);
    return string(dataBuffer.getData(), dataBuffer.getDataLen());
}

void NormalDocument::DeserializeFromString(const string& docStr)
{
    DataBuffer dataBuffer(const_cast<char*>(docStr.data()), docStr.length());
    dataBuffer.read(*this);
}

void NormalDocument::ModifyDocOperateType(DocOperateType op)
{
    mOpType = op;
    for (size_t i = 0; i < mSubDocuments.size(); ++i) {
        mSubDocuments[i]->ModifyDocOperateType(op);
    }
}

const std::string& NormalDocument::GetPrimaryKey() const
{
    if (mIndexDocument) {
        return mIndexDocument->GetPrimaryKey();
    }
    return Document::GetPrimaryKey();
}

size_t NormalDocument::GetMemoryUse() const
{
    size_t memUse = mPool->getUsedBytes();
    for (size_t i = 0; i < mSubDocuments.size(); ++i) {
        memUse += mSubDocuments[i]->GetMemoryUse();
    }
    return memUse;
}

size_t NormalDocument::EstimateMemory() const
{
    size_t ret = sizeof(*this);
    size_t pageSize = getpagesize();
    size_t pageAlignSize = (mPool->getUsedBytes() + pageSize - 1) / pageSize * pageSize;
    ret += pageAlignSize;
    for (size_t i = 0; i < mSubDocuments.size(); ++i) {
        ret += mSubDocuments[i]->EstimateMemory();
    }
    return ret;
}

std::unique_ptr<indexlibv2::document::NormalDocument> NormalDocument::ConstructNormalDocumentV2() const
{
    auto normalDocV2 = std::make_unique<indexlibv2::document::NormalDocument>(mPool);
    normalDocV2->_opType = mOpType;
    normalDocV2->_originalOpType = mOriginalOpType;
    normalDocV2->_serializedVersion = mSerializedVersion;
    normalDocV2->_ttl = mTTL;
    normalDocV2->_docInfo = mDocInfo;
    normalDocV2->_locatorV2 = mLocatorV2;
    normalDocV2->_userTimestamp = mUserTimestamp;
    normalDocV2->_segmentIdBeforeModified = mSegmentIdBeforeModified;
    normalDocV2->_modifyFailedFields = mModifyFailedFields;
    normalDocV2->_indexDocument = mIndexDocument;
    normalDocV2->_attributeDocument = mAttributeDocument;
    normalDocV2->_summaryDocument = mSummaryDocument;
    normalDocV2->_modifiedFields = mModifiedFields;
    normalDocV2->_trace = mTrace;
    return normalDocV2;
}

bool NormalDocument::HasFormatError() const
{
    for (size_t i = 0; i < mSubDocuments.size(); i++) {
        const NormalDocumentPtr& subDoc = mSubDocuments[i];
        AttributeDocumentPtr subAttrDoc;
        if (subDoc) {
            subAttrDoc = subDoc->GetAttributeDocument();
        }
        if (subAttrDoc && subAttrDoc->HasFormatError()) {
            return true;
        }
    }
    return mAttributeDocument && mAttributeDocument->HasFormatError();
}

bool NormalDocument::HasIndexUpdate() const
{
    if (mIndexDocument) {
        if (!mIndexDocument->GetModifiedTokens().empty()) {
            return true;
        }
    }
    for (const auto& doc : mSubDocuments) {
        if (doc->HasIndexUpdate()) {
            return true;
        }
    }
    return false;
}

void NormalDocument::RemoveSubDocument(size_t index)
{
    if (index < mSubDocuments.size()) {
        mSubDocuments.erase(mSubDocuments.begin() + index);
    }
}

bool NormalDocument::RemoveSubDocument(const std::string& pk)
{
    for (int i = 0; i < mSubDocuments.size(); ++i) {
        if (mSubDocuments[i]->GetPrimaryKey() == pk) {
            mSubDocuments.erase(mSubDocuments.begin() + i);
            return true;
        }
    }
    return false;
}

}} // namespace indexlib::document
