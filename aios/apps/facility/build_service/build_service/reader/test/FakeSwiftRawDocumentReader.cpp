#include "build_service/reader/test/FakeSwiftRawDocumentReader.h"

using namespace std;
using namespace build_service::document;
using namespace build_service::util;
namespace build_service { namespace reader {

BS_LOG_SETUP(reader, FakeSwiftRawDocumentReader);

FakeSwiftRawDocumentReader::FakeSwiftRawDocumentReader() : SwiftRawDocumentReader(SwiftClientCreatorPtr()) {}

FakeSwiftRawDocumentReader::~FakeSwiftRawDocumentReader() {}

bool FakeSwiftRawDocumentReader::getMaxTimestamp(int64_t& timestamp)
{
    timestamp = 12138;
    return true;
}

}} // namespace build_service::reader
