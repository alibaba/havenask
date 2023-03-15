#pragma once

#include <string>
#include "build_service/reader/ReaderModuleFactory.h"
#include "build_service/util/Log.h"

namespace build_service {
namespace reader {
class RawDocumentReader;
}  // namespace reader
}  // namespace build_service

namespace pluginplatform {
namespace reader_plugins {

class CustomReaderModuleFactory : public build_service::reader::ReaderModuleFactory
{
public:
    CustomReaderModuleFactory() {}
    virtual ~CustomReaderModuleFactory() {}
private:
    CustomReaderModuleFactory(const CustomReaderModuleFactory &);
    CustomReaderModuleFactory& operator=(const CustomReaderModuleFactory &);
public:
    /*override*/
    virtual bool init(const build_service::KeyValueMap &parameters) {
        return true;
    }

    /*override*/
    virtual void destroy() {
        delete this;
    }

    /*override reader*/
    virtual build_service::reader::RawDocumentReader* createRawDocumentReader(
            const std::string &readerName);

private:
    BS_LOG_DECLARE();
};

}}
