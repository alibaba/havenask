#pragma once

#include <string>

#include "build_service/common_define.h"
#include "build_service/reader/RawDocumentReader.h"
#include "build_service/reader/ReaderModuleFactory.h"
#include "build_service/util/Log.h"

namespace build_service { namespace reader {

class CustomRawDocumentReaderFactory : public ReaderModuleFactory
{
public:
    CustomRawDocumentReaderFactory();
    virtual ~CustomRawDocumentReaderFactory();

public:
    bool init(const KeyValueMap& parameters) override;
    void destroy() override;
    RawDocumentReader* createRawDocumentReader(const std::string& readerName) override;

private:
    BS_LOG_DECLARE();
};

}} // namespace build_service::reader
