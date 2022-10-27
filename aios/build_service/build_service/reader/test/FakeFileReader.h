#ifndef ISEARCH_BS_FAKEFILEREADER_H
#define ISEARCH_BS_FAKEFILEREADER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/reader/FileReader.h"

namespace build_service {
namespace reader {

class FakeFileReader : public FileReader
{
public:
    FakeFileReader();
    ~FakeFileReader();
private:
    FakeFileReader(const FakeFileReader &);
    FakeFileReader& operator=(const FakeFileReader &);
public:
    /* override */ bool get(char* output, uint32_t size, uint32_t& sizeUsed);
    void setFail(bool fail) {
        _fail = fail;
    }
private:
    bool _fail;
    BS_LOG_DECLARE();
};


}
}

#endif //ISEARCH_BS_FAKEFILEREADER_H
