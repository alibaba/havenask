#pragma once

#include "indexlib/file_system/package/PackageFileMeta.h"

namespace indexlib::file_system {

class PackageTestUtil
{
public:
    static bool CheckInnerFileMeta(const std::string& fileNodesDespStr, const PackageFileMeta& packageFileMeta);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::file_system
