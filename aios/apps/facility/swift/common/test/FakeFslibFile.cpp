#include "FakeFslibFile.h"

#include <assert.h>
#include <string>

namespace swift {
namespace storage {

AUTIL_LOG_SETUP(swift, FakeFslibFile);

FakeFslibFile::FakeFslibFile(fslib::fs::File *file) : fslib::fs::File(std::string(file->getFileName())) {
    assert(file);
    _seekPosLimit = -1;
    _file = file;
}

FakeFslibFile::~FakeFslibFile() { delete _file; }

} // namespace storage
} // namespace swift
