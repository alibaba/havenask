#include "build_service/reader/test/CustomizedRawDocParser.h"

#include <sstream>
#include <stddef.h>

#include "indexlib/config/ITabletSchema.h"
#include "indexlib/document/index_locator.h"
#include "indexlib/plugin/ModuleFactory.h"

using namespace std;
using namespace autil;

using namespace indexlib::document;
using namespace indexlib::plugin;
using namespace indexlib::config;

namespace build_service { namespace reader {

StringView MyRawDocument::EMPTY_STRING;

MyRawDocument::MyRawDocument()
{
    for (size_t i = 0; i < 4; i++) {
        mData[i] = 0.0;
    }
}

MyRawDocument::~MyRawDocument() {}

void MyRawDocument::setField(const StringView& fieldName, const StringView& fieldValue)
{
    assert(fieldValue.size() == 16);
    float* data = (float*)fieldValue.data();
    for (size_t i = 0; i < 4; i++) {
        mData[i] = data[i];
    }
}

const StringView& MyRawDocument::getField(const StringView& fieldName) const
{
    // assert(false);
    return EMPTY_STRING;
}

RawDocument* MyRawDocument::clone() const
{
    MyRawDocument* doc = new MyRawDocument();
    for (size_t i = 0; i < 4; i++) {
        doc->mData[i] = mData[i];
    }
    return doc;
}

void MyRawDocument::setData(const float* data)
{
    for (size_t i = 0; i < 4; i++) {
        mData[i] = data[i];
    }
}

string MyRawDocument::toString() const
{
    stringstream ss;
    for (size_t i = 0; i < 4; i++) {
        ss << mData[i] << " ";
    }
    return ss.str();
}

bool MyRawDocumentParser::parse(const string& docString, RawDocument& rawDoc)
{
    float* data = (float*)docString.c_str();
    MyRawDocument* doc = (MyRawDocument*)&rawDoc;
    doc->setData(data);
    return true;
}

extern "C" ModuleFactory* createDocumentFactory() { return new build_service::reader::MyDocumentFactory; }

extern "C" void destroyDocumentFactory(ModuleFactory* factory) { factory->destroy(); }

}} // namespace build_service::reader
