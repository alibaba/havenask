#include "indexlib/file_system/test/DirectoryChecker.h"

#include "autil/HashAlgorithm.h"
#include "autil/StringUtil.h"
#include "indexlib/base/PathUtil.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace autil;
using namespace indexlib::util;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(test, DirectoryChecker);

DirectoryChecker::FileHashes DirectoryChecker::GetAllFileHashes(const std::string& path,
                                                                const DirectoryChecker::ContentChanger& contentChanger)
{
    IFileSystemPtr fs = FileSystemCreator::CreateForRead("", path, FileSystemOptions::Offline()).GetOrThrow();
    DirectoryPtr directory = Directory::Get(fs);

    std::vector<std::string> physicalFileList;
    THROW_IF_FS_ERROR(FslibWrapper::ListDirRecursive(path, physicalFileList).Code(), "list dir [%s] failed",
                      path.c_str());
    for (const string& f : physicalFileList) {
        if (indexlibv2::PathUtil::IsValidSegmentDirName(f)) {
            THROW_IF_FS_ERROR(fs->MountSegment(f), "mount segment failed");
        }
    }
    fslib::FileList fileList;
    directory->ListDir("", fileList, true);

    vector<pair<uint64_t, string>> results;
    for (auto f : fileList) {
        if (directory->IsDir(f)) {
            results.emplace_back(0u, f);
            continue;
        }
        string content;
        directory->Load(f, content);
        string fileName = PathUtil::GetFileName(f);
        if (auto it = contentChanger.find(fileName); it != contentChanger.end()) {
            auto changer = it->second;
            if (!changer) {
                continue;
            }
            content = changer(content);
        }
        uint64_t hash = content == "" ? 0 : HashAlgorithm::hashString64(content.c_str(), content.length());
        results.emplace_back(hash, f);
    }
    sort(results.begin(), results.end(),
         [](pair<uint64_t, string> p1, pair<uint64_t, string> p2) -> bool { return p2.second < p1.second; });
    return results;
}

bool DirectoryChecker::FileNameMatchAnyPrefix(const string& filePath, const vector<string>& prefixes)
{
    for (const string& prefix : prefixes) {
        if (StringUtil::startsWith(PathUtil::GetFileName(filePath), prefix)) {
            return true;
        }
    }
    return false;
}

bool DirectoryChecker::CheckEqual(const DirectoryChecker::FileHashes& expectHashes,
                                  const DirectoryChecker::FileHashes& actualHashes)
{
    if (expectHashes.size() != actualHashes.size()) {
        DirectoryChecker::PrintDifference(expectHashes, actualHashes);
        return false;
    }
    for (size_t i = 0; i < expectHashes.size(); ++i) {
        if (expectHashes[i] != actualHashes[i]) {
            DirectoryChecker::PrintDifference(expectHashes, actualHashes);
            return false;
        }
    }
    // DirectoryChecker::PrintDifference(expectHashes, actualHashes);
    return true;
}

void DirectoryChecker::PrintDifference(const DirectoryChecker::FileHashes& expectHashes,
                                       const DirectoryChecker::FileHashes& actualHashes)
{
    map<string, uint64_t> expectMap, actualMap;
    set<string> expectSet, actualSet;
    for (auto item : expectHashes) {
        expectMap.insert(make_pair(item.second, item.first));
        expectSet.insert(item.second);
    }
    for (auto item : actualHashes) {
        actualMap.insert(make_pair(item.second, item.first));
        actualSet.insert(item.second);
    }

    cout << "==================================================" << endl;
    cout << "Difference of EXPECT [" << expectHashes.size()
         << " files] "
            "vs ACTUAL ["
         << actualHashes.size() << " files] " << endl;

    vector<string> tmpVector;
    cout << "===== Files only appear in expect: =====" << endl;
    set_difference(expectSet.begin(), expectSet.end(), actualSet.begin(), actualSet.end(),
                   inserter(tmpVector, tmpVector.begin()));
    for (string file : tmpVector) {
        cout << file << endl;
    }

    cout << "===== Files only appear in actual: =====" << endl;
    tmpVector.clear();
    set_difference(actualSet.begin(), actualSet.end(), expectSet.begin(), expectSet.end(),
                   inserter(tmpVector, tmpVector.begin()));
    for (string file : tmpVector) {
        cout << file << endl;
    }

    cout << "===== Files with differect hash: =====" << endl;
    tmpVector.clear();
    set_intersection(actualSet.begin(), actualSet.end(), expectSet.begin(), expectSet.end(),
                     inserter(tmpVector, tmpVector.begin()));
    vector<string> sameFiles;
    for (string file : tmpVector) {
        if (expectMap[file] != actualMap[file]) {
            cout << file << "  expect [" << expectMap[file]
                 << "]"
                    "  actual ["
                 << actualMap[file] << "]" << endl;
        } else {
            sameFiles.push_back(file);
        }
    }

    cout << "===== Same files: =====" << endl;
    for (string file : sameFiles) {
        cout << file << endl;
    }

    cout << "==================================================" << endl;
}
}} // namespace indexlib::file_system
