#include "build_service/reader/test/CustomRawDocumentReaderFactory.h"

#include "build_service/reader/test/CustomRawDocumentReader.h"

using namespace std;

namespace build_service { namespace reader {

BS_LOG_SETUP(reader, CustomRawDocumentReaderFactory);

CustomRawDocumentReaderFactory::CustomRawDocumentReaderFactory() {}

CustomRawDocumentReaderFactory::~CustomRawDocumentReaderFactory() {}

bool CustomRawDocumentReaderFactory::init(const KeyValueMap& parameters) { return true; }

void CustomRawDocumentReaderFactory::destroy() { delete this; }

RawDocumentReader* CustomRawDocumentReaderFactory::createRawDocumentReader(const string& readerName)
{
    if (readerName == "CustomRawDocumentReader") {
        CustomRawDocumentReader* rawDocumentReader = new CustomRawDocumentReader();
        return rawDocumentReader;
    } else {
        string errorMsg = "Invalid RawDocumentReader name: " + readerName + " for CustomRawDocumentReaderFactory";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return NULL;
    }
}

extern "C" plugin::ModuleFactory* createFactory_Reader() { return new CustomRawDocumentReaderFactory; }

extern "C" void destroyFactory_Reader(plugin::ModuleFactory* factory) { delete factory; }

}} // namespace build_service::reader
