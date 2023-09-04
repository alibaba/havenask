#include "indexlib/file_system/package/test/PackageTestUtil.h"

namespace indexlib::file_system {
AUTIL_LOG_SETUP(indexlib.file_system, PackageTestUtil);

bool PackageTestUtil::CheckInnerFileMeta(const std::string& fileNodesDespStr, const PackageFileMeta& packageFileMeta)
{
    std::vector<std::vector<std::string>> nodeInfos;
    autil::StringUtil::fromString(fileNodesDespStr, nodeInfos, ":", "#");
    if (nodeInfos.size() != packageFileMeta.GetInnerFileCount()) {
        AUTIL_LOG(ERROR, "nodeInfos.size()=%lu, packageFileMeta.GetInnerFileCount()=%lu", nodeInfos.size(),
                  packageFileMeta.GetInnerFileCount());
        return false;
    }
    InnerFileMeta::Iterator iter;
    size_t i = 0;
    bool res = true;
    for (iter = packageFileMeta.Begin(); iter != packageFileMeta.End(); ++iter, ++i) {
        assert(nodeInfos[i].size() == 5);
        if (nodeInfos[i][0] != iter->GetFilePath()) {
            res = false;
            AUTIL_LOG(ERROR, "nodeInfos[i][0]=%s, iter->GetFilePath()=%s", nodeInfos[i][0].c_str(),
                      iter->GetFilePath().c_str());
        }
        if (nodeInfos[i][1] == "DIR") {
            if (!iter->IsDir()) {
                res = false;
                AUTIL_LOG(ERROR, "nodeInfos[i][1]=%s, iter->IsDir()=%d", nodeInfos[i][1].c_str(), iter->IsDir());
            }
        } else {
            assert(nodeInfos[i][1] == "FILE");
            if (iter->IsDir()) {
                res = false;
                AUTIL_LOG(ERROR, "nodeInfos[i][1]=%s, iter->IsDir()=%d", nodeInfos[i][1].c_str(), iter->IsDir());
            }
        }
        if (autil::StringUtil::numberFromString<size_t>(nodeInfos[i][2]) != iter->GetLength()) {
            res = false;
            AUTIL_LOG(ERROR, "nodeInfos[i][2]=%s, iter->GetLength()=%lu", nodeInfos[i][2].c_str(), iter->GetLength());
        }
        if (autil::StringUtil::numberFromString<size_t>(nodeInfos[i][3]) != iter->GetOffset()) {
            res = false;
            AUTIL_LOG(ERROR, "nodeInfos[i][3]=%s, iter->GetOffset()=%lu", nodeInfos[i][3].c_str(), iter->GetOffset());
        }
        if (autil::StringUtil::numberFromString<size_t>(nodeInfos[i][4]) != iter->GetDataFileIdx()) {
            res = false;
            AUTIL_LOG(ERROR, "nodeInfos[i][4]=%s, iter->GetDataFileIdx()=%u", nodeInfos[i][4].c_str(),
                      iter->GetDataFileIdx());
        }
    }
    return res;
}

} // namespace indexlib::file_system
