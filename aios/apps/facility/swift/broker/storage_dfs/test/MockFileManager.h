#pragma once

#include "gmock/gmock.h"

#include "swift/broker/storage_dfs/FileManager.h"
#include "swift/util/FileManagerUtil.h"

namespace swift {
namespace storage {

class MockFileManager : public FileManager {
public:
    MockFileManager() {}

public:
    MOCK_METHOD0(refreshVersion, protocol::ErrorCode());

private:
    MOCK_METHOD2(doListFiles, fslib::ErrorCode(const fslib::FileList &, util::FileManagerUtil::FileLists &));
};

} // namespace storage
} // namespace swift
