#pragma once

#include <string>

#include "autil/Log.h" // IWYU pragma: keep
#include "indexlib/file_system/Directory.h"

namespace indexlib {
namespace index {

class IndexDirectoryCreator {
public:
    IndexDirectoryCreator();
    ~IndexDirectoryCreator();

public:
    static file_system::DirectoryPtr Create(const std::string &path);
};

} // namespace index
} // namespace indexlib
