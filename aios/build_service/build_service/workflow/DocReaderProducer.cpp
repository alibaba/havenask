#include "build_service/workflow/DocReaderProducer.h"
#include "build_service/reader/SwiftRawDocumentReader.h"

using namespace std;

using namespace build_service::reader;
using namespace build_service::document;
namespace build_service {
namespace workflow {

BS_LOG_SETUP(workflow, DocReaderProducer);

DocReaderProducer::DocReaderProducer(reader::RawDocumentReader *reader, uint64_t srcSignature)
    : _reader(reader)
    , _srcSignature(srcSignature)
    , _lastReadOffset(0)
{
}

DocReaderProducer::~DocReaderProducer() {
}

FlowError DocReaderProducer::produce(document::RawDocumentPtr &rawDocPtr) {
    int64_t offset;
    RawDocumentReader::ErrorCode ec = _reader->read(rawDocPtr, offset);
    _lastReadOffset.store(offset, std::memory_order_relaxed);
    if (RawDocumentReader::ERROR_NONE == ec) {
        common::Locator locator(_srcSignature, offset);
        rawDocPtr->setLocator(locator);
        return FE_OK;
    } else if (RawDocumentReader::ERROR_EOF == ec) {
        return FE_EOF;
    } else if (RawDocumentReader::ERROR_WAIT == ec ) {
        return FE_WAIT;
    } else if (RawDocumentReader::ERROR_PARSE == ec) {
        return FE_SKIP;
    } else if (RawDocumentReader::ERROR_EXCEPTION == ec) {
        return FE_FATAL;
    } else {
        return FE_EXCEPTION;
    }
}

bool DocReaderProducer::seek(const common::Locator &locator) {
    if(locator.getSrc() != _srcSignature) {
        BS_LOG(INFO, "do not seek, current src[%lu], seek src [%lu]",
               _srcSignature, locator.getSrc());
        return true;
    }
    BS_LOG(INFO, "seek offset [%ld]", locator.getOffset());
    return _reader->seek(locator.getOffset());
}

bool DocReaderProducer::stop() {
    return true;
}

bool DocReaderProducer::getMaxTimestamp(int64_t &timestamp) {
    SwiftRawDocumentReader *swiftReader = dynamic_cast<SwiftRawDocumentReader *>(_reader);
    if (!swiftReader) {
        BS_LOG(WARN, "getMaxTimestamp failed because has no swift reader.");
        return false;
    }
    return swiftReader->getMaxTimestampAfterStartTimestamp(timestamp);
}

bool DocReaderProducer::getLastReadTimestamp(int64_t &timestamp) {
    SwiftRawDocumentReader *swiftReader = dynamic_cast<SwiftRawDocumentReader *>(_reader);
    if (!swiftReader) {
        BS_LOG(WARN, "getLastReadTimestamp failed because has no swift reader.");
        return false;
    }
    timestamp = _lastReadOffset.load(std::memory_order_relaxed);
    return true;
}

}
}
