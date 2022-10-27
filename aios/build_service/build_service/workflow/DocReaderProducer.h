#ifndef ISEARCH_BS_DOCREADERPRODUCER_H
#define ISEARCH_BS_DOCREADERPRODUCER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/workflow/Producer.h"
#include "build_service/reader/RawDocumentReader.h"

namespace build_service {
namespace workflow {

class DocReaderProducer : public RawDocProducer
{
public:
    DocReaderProducer(reader::RawDocumentReader *reader, uint64_t srcSignature);
    virtual ~DocReaderProducer();
private:
    DocReaderProducer(const DocReaderProducer &);
    DocReaderProducer& operator=(const DocReaderProducer &);
public:
    /* override */ FlowError produce(document::RawDocumentPtr &rawDoc);
    /* override */ bool seek(const common::Locator &locator);
    /* override */ bool stop();
public:
    virtual bool getMaxTimestamp(int64_t &timestamp);
    virtual bool getLastReadTimestamp(int64_t &timestamp);
    uint64_t getLocatorSrc() const { return _srcSignature; }

private:
    reader::RawDocumentReader *_reader;
    uint64_t _srcSignature;
    std::atomic<int64_t> _lastReadOffset;
private:
    BS_LOG_DECLARE();
};

}
}

#endif //ISEARCH_BS_DOCREADERPRODUCER_H
