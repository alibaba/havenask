#include "indexlib/index/test/IndexTestUtil.h"

#include "indexlib/file_system/fslib/FslibWrapper.h"

namespace indexlibv2 { namespace index {
AUTIL_LOG_SETUP(indexlib.index, IndexTestUtil);

IndexTestUtil::ToDelete IndexTestUtil::deleteFuncs[IndexTestUtil::DM_MAX_MODE] = {
    IndexTestUtil::NoDelete,   IndexTestUtil::DeleteEven, IndexTestUtil::DeleteSome,
    IndexTestUtil::DeleteMany, IndexTestUtil::DeleteAll,
};

void IndexTestUtil::ResetDir(const std::string& dir)
{
    CleanDir(dir);
    MkDir(dir);
}

void IndexTestUtil::MkDir(const std::string& dir)
{
    if (!indexlib::file_system::FslibWrapper::IsExist(dir).GetOrThrow()) {
        try {
            indexlib::file_system::FslibWrapper::MkDirE(dir, true);
        } catch (const indexlib::util::FileIOException&) {
            AUTIL_LOG(ERROR, "Create work directory [%s] failed", dir.c_str());
        }
    }
}

void IndexTestUtil::CleanDir(const std::string& dir)
{
    if (indexlib::file_system::FslibWrapper::IsExist(dir).GetOrThrow()) {
        try {
            indexlib::file_system::FslibWrapper::DeleteDirE(dir, indexlib::file_system::DeleteOption::NoFence(false));
        } catch (const indexlib::util::FileIOException&) {
            AUTIL_LOG(ERROR, "Remove directory [%s] failed", dir.c_str());
            assert(false);
        }
    }
}

}} // namespace indexlibv2::index
