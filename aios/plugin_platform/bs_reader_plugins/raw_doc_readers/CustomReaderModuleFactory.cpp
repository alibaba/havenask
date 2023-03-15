#include "CustomReaderModuleFactory.h"
#include "KafkaRawDocumentReader.h"

using namespace build_service::reader;

namespace pluginplatform {
namespace reader_plugins {

BS_LOG_SETUP(reader_plugins, CustomReaderModuleFactory);

RawDocumentReader* CustomReaderModuleFactory::createRawDocumentReader(
        const std::string &readerName)
{
    if ( readerName == "kafka" ){
        BS_LOG(INFO, "create reader[%s] succuss", readerName.c_str());
        return new KafkaRawDocumentReader;
    } else {
        BS_LOG(ERROR, "not supported reader[%s]", readerName.c_str());
        return nullptr;
    }
}

extern "C" build_service::plugin::ModuleFactory* createFactory_Reader()
{
    return (build_service::plugin::ModuleFactory*) new (std::nothrow) pluginplatform::reader_plugins::CustomReaderModuleFactory();
}

extern "C" void destroyFactory_Reader(build_service::plugin::ModuleFactory *factory)
{
    factory->destroy();
}

}
}
