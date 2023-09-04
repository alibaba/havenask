#include "build_service/reader/test/FakeFileReader.h"

using namespace std;

namespace build_service { namespace reader {
BS_LOG_SETUP(reader, FakeFileReader);

FakeFileReader::FakeFileReader() : _fail(false) {}

FakeFileReader::~FakeFileReader() {}

bool FakeFileReader::get(char* output, uint32_t size, uint32_t& sizeUsed)
{
    if (_fail) {
        return false;
    }
    return FileReader::get(output, size, sizeUsed);
}

}} // namespace build_service::reader
