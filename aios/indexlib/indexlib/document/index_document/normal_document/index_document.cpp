#include "indexlib/document/index_document/normal_document/index_document.h"
#include "indexlib/misc/exception.h"
using namespace autil;
using namespace autil::mem_pool;
using namespace std;

IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(document);
IE_LOG_SETUP(document, IndexDocument);

IndexDocument::IndexDocument(Pool* pool) 
    : mFieldCount(0)
    , mDocId(INVALID_DOCID)
    , mPool(pool)
    , mPayloads(mPool, HASH_MAP_INIT_ELEM_COUNT)
    , mTermPayloads(mPool, HASH_MAP_INIT_ELEM_COUNT)
{
}

IndexDocument::~IndexDocument()
{
    size_t size = mFields.size();
    for ( size_t i = 0; i < size; i++ )
    {
        if ( mFields[i] )
        {
            IE_POOL_COMPATIBLE_DELETE_CLASS(mPool, mFields[i]);
            mFields[i] = NULL;
        }
    }
    mFields.clear();
}

Field* IndexDocument::CreateField(fieldid_t fieldId, Field::FieldTag fieldTag)
{
    assert(fieldId != INVALID_FIELDID);

    if ((fieldid_t)mFields.size() > fieldId)
    {
        if (NULL == mFields[fieldId])
        {
            mFields[fieldId] = CreateFieldByTag(mPool, fieldTag, false);
            mFields[fieldId]->SetFieldId(fieldId);
            ++mFieldCount;
        }
        else if(mFields[fieldId]->GetFieldId() == INVALID_FIELDID)
        {
            ++mFieldCount;
            mFields[fieldId]->SetFieldId(fieldId);
        }
        return mFields[fieldId];
    }

    mFields.resize(fieldId + 1,NULL);
    Field* pField = CreateFieldByTag(mPool, fieldTag, false);
    pField->SetFieldId(fieldId);
    mFields[fieldId] = pField;
    ++mFieldCount;
    return pField;
}

void IndexDocument::ClearField(fieldid_t fieldId)
{
    if ((fieldid_t)mFields.size() <= fieldId)
    {
        return;
    }
    
    if (NULL == mFields[fieldId])
    {
        return;
    }

    if (mFields[fieldId]->GetFieldId() != INVALID_FIELDID)
    {
        --mFieldCount;
    }
    IE_POOL_COMPATIBLE_DELETE_CLASS(mPool, mFields[fieldId]);    
    mFields[fieldId] = NULL;
}

void IndexDocument::ClearData()
{
    size_t size = mFields.size();
    for ( size_t i = 0; i < size; i++ )
    {
        if ( mFields[i] )
        {
            IE_POOL_COMPATIBLE_DELETE_CLASS(mPool, mFields[i]);
            mFields[i] = NULL;
        }
    }
    mFields.clear();
    mFieldCount = 0;
    mDocId = INVALID_DOCID;

    mSectionAttributeVec.clear();
    mPayloads.Clear();
    mTermPayloads.Clear();
    mPrimaryKey.clear();
}

bool IndexDocument::AddField(Field* field)
{
    assert(field && field->GetFieldId() != INVALID_FIELDID);
    
    fieldid_t fieldId = field->GetFieldId();
    if ((fieldid_t)mFields.size() <= fieldId)
    {
        mFields.resize(fieldId + 1,NULL);
    }
    
    mFields[fieldId] = field;
    ++mFieldCount;
    return true;
}

Field* IndexDocument::GetField(fieldid_t fieldId)
{
    return const_cast<Field*>(
        static_cast<const IndexDocument&>(*this).GetField(fieldId));
}

const Field* IndexDocument::GetField(fieldid_t fieldId) const
{
    if (fieldId >= (fieldid_t)mFields.size())
    {
        return NULL;
    }
    return mFields[fieldId];
}

void IndexDocument::SetField(fieldid_t fieldId, Field* field)
{
    assert(fieldId != INVALID_FIELDID);
    if (!field)
    {
	return;
    }
    
    if ((fieldid_t)mFields.size() <= fieldId)
    {
        mFields.resize(fieldId + 1,NULL);
    }

    if (!mFields[fieldId])
    {
        ++mFieldCount;
    }
    else
    {
        IE_POOL_COMPATIBLE_DELETE_CLASS(mPool, mFields[fieldId]);
    }
    
    mFields[fieldId] = field;
    field->SetFieldId(fieldId);
}

docpayload_t IndexDocument::GetDocPayload(const string& termText) const
{
    docpayload_t ans = 0;
    return mPayloads.Find(HashAlgorithm::hashString64(termText.c_str()), ans);
}

docpayload_t IndexDocument::GetDocPayload(uint64_t intKey) const
{
    docpayload_t ans = 0;
    return mPayloads.Find(intKey, ans);
}

termpayload_t IndexDocument::GetTermPayload(const string& termText) const
{
    uint32_t ans = 0;
    return mTermPayloads.Find(HashAlgorithm::hashString64(termText.c_str()), ans);
}

termpayload_t IndexDocument::GetTermPayload(uint64_t intKey) const
{
    uint32_t ans = 0;
    return mTermPayloads.Find(intKey, ans);
}

bool IndexDocument::operator==(const IndexDocument& doc)
{
    if (this == &doc)
        return true;
    if (mDocId != doc.mDocId 
        || mFieldCount != doc.mFieldCount 
        || mFields.size() != doc.mFields.size() 
        || mPrimaryKey != doc.mPrimaryKey)
    {
        return false;
    }
    for (uint32_t i = 0; i < mFields.size(); ++i)
    {
        bool b1 = mFields[i] == NULL && doc.mFields[i] == NULL ;
        bool b2 = mFields[i] != NULL && doc.mFields[i] != NULL && *mFields[i] == *doc.mFields[i];
        if (b1 || b2)
            continue;
        return false;
    }
    return true;
}

void IndexDocument::SetTermPayload(const std::string& termText, termpayload_t payload)
{ 
    mTermPayloads.FindAndInsert(HashAlgorithm::hashString64(termText.c_str()), payload);
}

void IndexDocument::SetTermPayload(uint64_t intKey, termpayload_t payload)
{ 
    mTermPayloads.FindAndInsert(intKey, payload);
}

void IndexDocument::SetDocPayload(const std::string& termText, docpayload_t docPayload) 
{ 
    mPayloads.FindAndInsert(HashAlgorithm::hashString64(termText.c_str()), docPayload);
}

void IndexDocument::SetDocPayload(uint64_t intKey, docpayload_t docPayload)
{ 
    mPayloads.FindAndInsert(intKey, docPayload);
}

void IndexDocument::CreateSectionAttribute(indexid_t indexId, const string& attrStr)
{
    ConstString value(attrStr, mPool);
    SetSectionAttribute(indexId, value);
}

void IndexDocument::SetSectionAttribute(indexid_t indexId, const ConstString& attrStr)
{
    assert(indexId != INVALID_INDEXID);
    if (attrStr == ConstString::EMPTY_STRING)
    {
        return;
    }

    if ((indexid_t)mSectionAttributeVec.size() <= indexId)
    {
        mSectionAttributeVec.resize(indexId + 1, ConstString::EMPTY_STRING);
    }
    mSectionAttributeVec[indexId] = attrStr;
}

const ConstString& IndexDocument::GetSectionAttribute(indexid_t indexId) const
{
    if (indexId >= (indexid_t)mSectionAttributeVec.size())
    {
        return ConstString::EMPTY_STRING;
    }
    return mSectionAttributeVec[indexId];
}

void IndexDocument::serialize(DataBuffer &dataBuffer) const
{
    SerializeFieldVector(dataBuffer, mFields);
    dataBuffer.write(mPrimaryKey);
    SerializeHashMap(dataBuffer, mPayloads);
    SerializeHashMap(dataBuffer, mTermPayloads);
    dataBuffer.write(mSectionAttributeVec);
}

void IndexDocument::deserialize(DataBuffer &dataBuffer, mem_pool::Pool *pool,
                                uint32_t docVersion)
{
    mFieldCount = DeserializeFieldVector(dataBuffer, mFields, mPool, (docVersion <= 4));
    dataBuffer.read(mPrimaryKey);
    DeserializeHashMap(dataBuffer, mPayloads);
    DeserializeHashMap(dataBuffer, mTermPayloads);
    dataBuffer.read(mSectionAttributeVec, mPool);
}

template<typename K, typename V>
void IndexDocument::SerializeHashMap(DataBuffer &dataBuffer, const util::HashMap<K, V> &hashMap)
{
    typedef typename util::HashMap<K, V> HashMapType;
    typename HashMapType::Iterator it = hashMap.CreateIterator();
    dataBuffer.write(hashMap.Size());
    while (it.HasNext())
    {
        typename HashMapType::KeyValuePair &p = it.Next();
        dataBuffer.write(p.first);
        dataBuffer.write(p.second);
    }
}

template<typename K, typename V>
void IndexDocument::DeserializeHashMap(DataBuffer &dataBuffer, util::HashMap<K, V> &hashMap)
{
    size_t size;
    dataBuffer.read(size);
    hashMap.Clear();
    while (size--)
    {
        K k;
        V v;
        dataBuffer.read(k);
        dataBuffer.read(v);
        hashMap.FindAndInsert(k, v);
    }
}

void IndexDocument::SerializeFieldVector(DataBuffer &dataBuffer,
        const FieldVector& fields)
{
    uint32_t size = fields.size();
    dataBuffer.write(size);
    for (uint32_t i = 0; i < size; ++i)
    {
        uint8_t descriptor = GenerateFieldDescriptor(fields[i]);
        dataBuffer.write(descriptor);
        if (descriptor != 0)
        {
            dataBuffer.write(*(fields[i]));
        }
    }
}

uint32_t IndexDocument::DeserializeFieldVector(
        DataBuffer &dataBuffer, FieldVector& fields, Pool *pool, bool isLegacy)
{
    uint32_t size = 0;
    dataBuffer.read(size);
    fields.resize(size);
    uint32_t fieldCount = 0;
    for (uint32_t i = 0; i < size; ++i)
    {
        bool fieldExist = false;
        Field::FieldTag fieldTag = Field::FieldTag::TOKEN_FIELD;
        uint8_t descriptor = 0;
        dataBuffer.read(descriptor);
        fieldExist = (descriptor != 0);
        
        if (fieldExist && !isLegacy)
        {
            fieldTag = GetFieldTagFromFieldDescriptor(descriptor);
        }

        if (!fieldExist)
        {
            fields[i] = NULL;
        }
        else
        {
            fields[i] = CreateFieldByTag(pool, fieldTag, true);
            if (NULL == fields[i])
            {
                INDEXLIB_FATAL_ERROR(
                        DocumentDeserialize, "invalid fieldTag[%d]",
                        static_cast<int8_t>(fieldTag));
            }
            dataBuffer.read(*(fields[i]));
            if (fields[i]->GetFieldId() != INVALID_FIELDID)
            {
                fieldCount++;
            }            
        }
    }
    return fieldCount;
}

IE_NAMESPACE_END(document);

