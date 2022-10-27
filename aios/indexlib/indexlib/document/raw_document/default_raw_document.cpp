#include <algorithm>
#include <autil/StringUtil.h>
#include "indexlib/document/raw_document/default_raw_document.h"
#include "indexlib/document/raw_document/raw_document_define.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

IE_NAMESPACE_BEGIN(document);
IE_LOG_SETUP(document, DefaultRawDocument);

std::string DefaultRawDocument::RAW_DOC_IDENTIFIER = "__@__default_raw_documment";

DefaultRawDocument::DefaultRawDocument(const DefaultRawDocument &other)
    : RawDocument(other)
    , mHashMapManager(other.mHashMapManager)
    , mHashMapPrimary(other.mHashMapPrimary) // primary: shallow copy
    , mHashMapIncrement(other.mHashMapIncrement->clone()) //increment: deep copy
    , mFieldCount(other.mFieldCount)
{
    // all the ConstStrings of value: deep copy
    for (FieldVec::const_iterator it = other.mFieldsPrimary.begin();
         it != other.mFieldsPrimary.end(); ++it)
    {
        if (it->data()) {
            ConstString fieldValue(*it, mPool);
            mFieldsPrimary.push_back(fieldValue);
        } else {
            mFieldsPrimary.push_back(ConstString());
        }
    }
    for (FieldVec::const_iterator it = other.mFieldsIncrement.begin();
         it != other.mFieldsIncrement.end(); ++it)
    {
        if (it->data()) {
            ConstString fieldValue(*it, mPool);
            mFieldsIncrement.push_back(fieldValue);
        } else {
            mFieldsIncrement.push_back(ConstString());
        }
    }
}

string DefaultRawDocument::toString() const {
    stringstream ss;
    if (mHashMapPrimary) {
        const FieldVec &keyFields1 = mHashMapPrimary->getKeyFields();
        assert(keyFields1.size() == mFieldsPrimary.size());
        for (size_t i = 0; i < mFieldsPrimary.size(); ++i) {
            if (mFieldsPrimary[i].c_str() && keyFields1[i].c_str()) {
                ss << keyFields1[i] << " = " << mFieldsPrimary[i] << endl;
            }
        }
    }
    
    const FieldVec &keyFields2 = mHashMapIncrement->getKeyFields();
    assert(keyFields2.size() == mFieldsIncrement.size());
    for (size_t i = 0; i < mFieldsIncrement.size(); ++i) {
        if (mFieldsIncrement[i].c_str() && keyFields2[i].c_str()) {
            ss << keyFields2[i] << " = " << mFieldsIncrement[i] << endl;
        }
    }
    return ss.str();
}

DocOperateType DefaultRawDocument::getDocOperateType(const ConstString &cmdString) {
    if (cmdString.empty()) {
        return ADD_DOC;
    } else if (cmdString == CMD_ADD) {
        return ADD_DOC;
    } else if (cmdString == CMD_DELETE) {
        return DELETE_DOC;
    } else if (cmdString == CMD_DELETE_SUB) {
        return DELETE_SUB_DOC;
    } else if (cmdString == CMD_UPDATE_FIELD) {
        return UPDATE_FIELD;
    } else if (cmdString == CMD_SKIP) {
        return SKIP_DOC;
    } else {
        return UNKNOWN_OP;
    }
    return UNKNOWN_OP;
}

DefaultRawDocument *DefaultRawDocument::clone() const {
    return new DefaultRawDocument(*this);
}

DefaultRawDocument *DefaultRawDocument::createNewDocument() const {
    return new DefaultRawDocument(mHashMapManager);
}

void DefaultRawDocument::setField(const ConstString &fieldName,
                                  const ConstString &fieldValue)
{
    ConstString copyedValue(fieldValue, mPool);
    ConstString *value = search(fieldName);
    if (value) {
        // if the KEY is in the map, then just record the VALUE.
        if (value->data() == NULL) {
            mFieldCount++;
        }
        *value = copyedValue;
    }
    else {
        // or record both KEY and VALUE.
        addNewField(fieldName, copyedValue);
    }
}

void DefaultRawDocument::setFieldNoCopy(const ConstString &fieldName,
                                        const ConstString &fieldValue)
{
    assert(mPool->isInPool(fieldValue.data()));
    ConstString *value = search(fieldName);    
    if (value) {
        if (value->data() == NULL) {
            mFieldCount++;
        }
        *value = fieldValue;
    }
    else {
        addNewField(fieldName, fieldValue);
    }
}

const ConstString &DefaultRawDocument::getField(
        const ConstString &fieldName) const
{
    const ConstString *fieldValue = search(fieldName);
    if (fieldValue==NULL || fieldValue->data()==NULL) {
        return RawDocument::EMPTY_STRING;
    }
    return *fieldValue;
}

// search: return NULL if the KEY haven't been seen.
// Note that because sharing hash map,
// returning a valid pointer does NOT mean the VALUE is set in this doc.
ConstString *DefaultRawDocument::search(const ConstString &fieldName)
{
    size_t id = KeyMap::INVALID_INDEX;
    if (mHashMapPrimary) {
        id = mHashMapPrimary->find(fieldName);
        if (id < mFieldsPrimary.size()) {
            return &mFieldsPrimary[id];
        }
    }
    
    id = mHashMapIncrement->find(fieldName);
    if (id < mFieldsIncrement.size()) {
        return &mFieldsIncrement[id];
    }
    return NULL;
}

const ConstString *DefaultRawDocument::search(
        const ConstString &fieldName) const
{
    size_t id = KeyMap::INVALID_INDEX;
    if (mHashMapPrimary) {
        id = mHashMapPrimary->find(fieldName);
        if (id < mFieldsPrimary.size()) {
            return &mFieldsPrimary[id];
        }
    }
    
    id = mHashMapIncrement->find(fieldName);
    if (id < mFieldsIncrement.size()) {
        return &mFieldsIncrement[id];
    }
    return NULL;
}

void DefaultRawDocument::addNewField(const ConstString &fieldName,
                                     const ConstString &fieldValue)
{
    mHashMapIncrement->insert(fieldName);
    mFieldsIncrement.push_back(fieldValue);
    mFieldCount++;
}

bool DefaultRawDocument::exist(const ConstString &fieldName) const
{
    const ConstString *ptr = search(ConstString(fieldName));
    return ptr != NULL && ptr->data() != NULL;
}

void DefaultRawDocument::eraseField(const ConstString &fieldName)
{
    ConstString *value = search(fieldName);
    if (value) {
        *value = RawDocument::EMPTY_STRING;
        mFieldCount--;
    }
}

uint32_t DefaultRawDocument::getFieldCount() const
{
    return mFieldCount;
}

void DefaultRawDocument::clear()
{
    mFieldCount = 0;
    mHashMapPrimary = mHashMapManager->getHashMapPrimary();
    mFieldsPrimary.clear();
    if (mHashMapPrimary) {
        mFieldsPrimary.resize(mHashMapPrimary->size());
    }
    mFieldsIncrement.clear();
    mHashMapIncrement.reset(new KeyMap());
}

DocOperateType DefaultRawDocument::getDocOperateType()
{
    if (mOpType != UNKNOWN_OP) {
        return mOpType;
    }
    const ConstString& cmd = getField(ConstString(CMD_TAG));
    mOpType = getDocOperateType(cmd);
    return mOpType;
}

size_t DefaultRawDocument::EstimateMemory() const
{
    return sizeof(*this) + mPool ? mPool->getTotalBytes() : 0;
}

IE_NAMESPACE_END(document);
