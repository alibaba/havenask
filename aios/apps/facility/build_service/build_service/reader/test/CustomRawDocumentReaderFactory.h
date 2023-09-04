#ifndef CUSTOMRAWDOCUMENT_READER_FACTORY_H
#define CUSTOMRAWDOCUMENT_READER_FACTORY_H

#include "build_service/reader/RawDocumentReader.h"
#include "build_service/reader/ReaderModuleFactory.h"

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

#endif // GENERATIONMETA_PROCESSOR_FACTORY_H
