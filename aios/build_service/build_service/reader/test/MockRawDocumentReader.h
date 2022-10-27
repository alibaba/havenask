#ifndef ISEARCH_BS_MOCKRAWDOCUMENTREADER_H
#define ISEARCH_BS_MOCKRAWDOCUMENTREADER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/reader/RawDocumentReader.h"
#include "build_service/test/unittest.h"

namespace build_service {
namespace reader {

class MockRawDocumentReader : public RawDocumentReader
{
public:
    using RawDocumentReader::ErrorCode;
public:
    MockRawDocumentReader() {
        ON_CALL(*this, init(_)).WillByDefault(Return(true));
        ON_CALL(*this, seek(_)).WillByDefault(Return(true));
        ON_CALL(*this, readDocStr(_, _, _)).WillByDefault(Return(ERROR_EOF));
        ON_CALL(*this, isEof()).WillByDefault(Return(true));
    }
    ~MockRawDocumentReader() {}
private:
    MockRawDocumentReader(const MockRawDocumentReader &);
    MockRawDocumentReader& operator=(const MockRawDocumentReader &);
public:
    MOCK_METHOD1(init, bool(const ReaderInitParam &params));
    MOCK_METHOD3(readDocStr, ErrorCode(std::string &, int64_t &, int64_t &));
    MOCK_METHOD1(seek, bool(int64_t));
    MOCK_CONST_METHOD0(isEof, bool());
    MOCK_METHOD2(suspendReadAtTimestamp, void(int64_t, ExceedTsAction));
    MOCK_CONST_METHOD0(isExceedSuspendTimestamp, bool()); 
private:
    BS_LOG_DECLARE();
};

typedef ::testing::StrictMock<MockRawDocumentReader> StrictMockRawDocumentReader;
typedef ::testing::NiceMock<MockRawDocumentReader> NiceMockRawDocumentReader;

}
}

#endif //ISEARCH_BS_MOCKRAWDOCUMENTREADER_H
