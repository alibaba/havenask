#include <algorithm>
#include <autil/StringUtil.h>
#include "indexlib/document/raw_document.h"
#include "indexlib/document/raw_document/raw_document_define.h"
#include "indexlib/misc/exception.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(document);
IE_LOG_SETUP(document, RawDocument);

const ConstString RawDocument::EMPTY_STRING = ConstString();

RawDocument::RawDocument()
    : mPool(new autil::mem_pool::Pool(1024))
{}

RawDocument::RawDocument(const RawDocument &other)
    : Document(other)
    , mPool(new Pool(1024))
{}

RawDocument::~RawDocument()
{
    DELETE_AND_SET_NULL(mPool);
}

void RawDocument::setField(const char *fieldName, size_t nameLen,
                           const char *fieldValue, size_t valueLen)
{
    ConstString name(fieldName, nameLen);
    ConstString value(fieldValue, valueLen);
    setField(name, value);
}

void RawDocument::setField(const string &fieldName, const string &fieldValue)
{
    setField(fieldName.data(), fieldName.length(),
             fieldValue.data(), fieldValue.length());
}

string RawDocument::getField(const string &fieldName) const
{
    const ConstString &constStr = getField(ConstString(fieldName));
    if (constStr.data()) {
        return string(constStr.data(), constStr.size());
    }
    return string(EMPTY_STRING.data(), EMPTY_STRING.size());
}

bool RawDocument::exist(const string &fieldName) const
{
    return exist(ConstString(fieldName));
}

void RawDocument::eraseField(const string &fieldName)
{
    eraseField(ConstString(fieldName));
}

void RawDocument::setLocator(const IndexLocator &locator)
{
    mLocator.SetLocator(locator.toString());
}

IndexLocator RawDocument::getLocator() const
{
    IndexLocator ret;
    if (!ret.fromString(mLocator.GetLocator()))
    {
        return INVALID_INDEX_LOCATOR;
    }
    return ret;
}

void RawDocument::DoSerialize(autil::DataBuffer &dataBuffer,
                              uint32_t serializedVersion) const
{
    INDEXLIB_THROW(misc::UnSupportedException,
                   "raw document does not DoSerialize");
}
void RawDocument::DoDeserialize(autil::DataBuffer &dataBuffer,
                                uint32_t serializedVersion)
{
    INDEXLIB_THROW(misc::UnSupportedException,
                   "raw document does not DoDeserialize");
}

void RawDocument::SetDocId(docid_t docId)
{
    INDEXLIB_THROW(misc::UnSupportedException,
                   "raw document does not set doc id");    
}

bool RawDocument::NeedTrace() const
{
    return !GetTraceId().empty();
}

string RawDocument::GetTraceId() const
{
    return getField(BUILTIN_KEY_TRACE_ID);
}

IE_NAMESPACE_END(document);

