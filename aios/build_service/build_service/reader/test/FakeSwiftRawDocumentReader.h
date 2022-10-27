#ifndef ISEARCH_BS_FAKESWIFTRAWDOCUMENTREADER_H
#define ISEARCH_BS_FAKESWIFTRAWDOCUMENTREADER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/reader/SwiftRawDocumentReader.h"

namespace build_service {
namespace reader {

class FakeSwiftRawDocumentReader : public SwiftRawDocumentReader
{
public:
    FakeSwiftRawDocumentReader();
    ~FakeSwiftRawDocumentReader();
private:
    FakeSwiftRawDocumentReader(const FakeSwiftRawDocumentReader &);
    FakeSwiftRawDocumentReader& operator=(const FakeSwiftRawDocumentReader &);
public:
    /*override*/ bool getMaxTimestamp(int64_t &timestamp);
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(FakeSwiftRawDocumentReader);

}
}

#endif //ISEARCH_BS_FAKESWIFTRAWDOCUMENTREADER_H
