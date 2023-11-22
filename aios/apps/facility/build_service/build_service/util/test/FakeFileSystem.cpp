#include <iosfwd>
#include <string>

#include "fslib/common/common_type.h"
#include "fslib/fs/FileSystem.h"
#include "unittest/unittest.h"

using namespace std;

bool removeFail = false;
void setRemove(bool fail) { removeFail = fail; }
bool copyFail = false;
void setCopy(bool fail) { copyFail = fail; }
bool renameFail = false;
void setRename(bool fail) { renameFail = fail; }
bool isExistFail = false;
void setIsExist(bool fail) { isExistFail = fail; }

namespace fslib { namespace fs {

ErrorCode FileSystem::remove(const std::string& path)
{
    if (removeFail) {
        return EC_UNKNOWN;
    }
    typedef ErrorCode (*func_type)(const string&);
    func_type func = GET_NEXT_FUNC(func_type);
    return func(path);
}

ErrorCode FileSystem::isExist(const std::string& path)
{
    if (isExistFail) {
        return EC_UNKNOWN;
    }
    typedef ErrorCode (*func_type)(const string&);
    func_type func = GET_NEXT_FUNC(func_type);
    return func(path);
}

ErrorCode FileSystem::rename(const std::string& src, const std::string& dst)
{
    if (renameFail) {
        return EC_UNKNOWN;
    }
    typedef ErrorCode (*func_type)(const string&, const string&);
    func_type func = GET_NEXT_FUNC(func_type);
    return func(src, dst);
}

ErrorCode FileSystem::copy(const std::string& src, const std::string& dst)
{
    if (copyFail) {
        return EC_UNKNOWN;
    }
    typedef ErrorCode (*func_type)(const string&, const string&);
    func_type func = GET_NEXT_FUNC(func_type);
    return func(src, dst);
}

}} // namespace fslib::fs
