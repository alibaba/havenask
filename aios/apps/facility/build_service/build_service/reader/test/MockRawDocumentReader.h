#pragma once

#include "build_service/common_define.h"
#include "build_service/reader/RawDocumentReader.h"
#include "build_service/test/unittest.h"
#include "build_service/util/Log.h"

namespace build_service { namespace reader {

class MockRawDocumentReader : public RawDocumentReader
{
public:
    using RawDocumentReader::ErrorCode;

public:
    MockRawDocumentReader()
    {
        ON_CALL(*this, init(_)).WillByDefault(Return(true));
        ON_CALL(*this, seek(_)).WillByDefault(Return(true));
        ON_CALL(*this, readDocStr(_, _, _)).WillByDefault(Return(ERROR_EOF));
        ON_CALL(*this, isEof()).WillByDefault(Return(true));
    }
    ~MockRawDocumentReader() {}

private:
    MockRawDocumentReader(const MockRawDocumentReader&);
    MockRawDocumentReader& operator=(const MockRawDocumentReader&);

public:
    MOCK_METHOD1(init, bool(const ReaderInitParam& params));
    MOCK_METHOD2(read, ErrorCode(document::RawDocument&, Checkpoint*));
    MOCK_METHOD3(readDocStr, ErrorCode(std::string&, Checkpoint*, DocInfo&));
    MOCK_METHOD1(seek, bool(const Checkpoint&));
    MOCK_CONST_METHOD0(isEof, bool());
    MOCK_METHOD2(suspendReadAtTimestamp, void(int64_t, common::ExceedTsAction));
    MOCK_CONST_METHOD0(isExceedSuspendTimestamp, bool());

private:
    BS_LOG_DECLARE();
};

typedef ::testing::StrictMock<MockRawDocumentReader> StrictMockRawDocumentReader;
typedef ::testing::NiceMock<MockRawDocumentReader> NiceMockRawDocumentReader;

}} // namespace build_service::reader
