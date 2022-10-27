#include <autil/StringUtil.h>
#include <autil/HashAlgorithm.h>
#include "indexlib/test/directory_checker.h"
#include "indexlib/util/path_util.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/file_system/directory_creator.h"
#include "indexlib/storage/file_system_wrapper.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_BEGIN(test);
IE_LOG_SETUP(test, DirectoryChecker);

DirectoryChecker::DirectoryChecker() 
{
}

DirectoryChecker::~DirectoryChecker() 
{
}

DirectoryChecker::FileHashes DirectoryChecker::GetAllFileHashes(
    const string& path, const std::vector<std::string>& excludeFilesPrefix)
{
    DirectoryPtr directory = DirectoryCreator::Create(path);
    fslib::FileList physicalFileList;
    FileSystemWrapper::ListDirRecursive(path, physicalFileList);
    for (const string& f : physicalFileList)
    {
        string filePath = FileSystemWrapper::JoinPath(path, f);
        if (FileSystemWrapper::IsDir(filePath))
        {
            directory->MountPackageFile(f + PACKAGE_FILE_PREFIX);
        }
    }
    fslib::FileList fileList;
    directory->ListFile("", fileList, true);  // all files, include physical & logical
    
    vector<pair<uint64_t, string>> results;
    for (auto f : fileList)
    {
        if (f == "segment_10_level_0/package_file.__data__0")
        {
            cout << "her" << endl;
        }
        if (FileNameMatchAnyPrefix(f, excludeFilesPrefix))
        {
            continue;
        }
        if (directory->IsDir(f))
        {
            results.emplace_back(0u, f);
            continue;
        }
        string content;
        directory->Load(f, content);
        uint64_t hash = content == "" ? 0 : HashAlgorithm::hashString64(content.c_str(), content.length());
        results.emplace_back(hash, f);
    }
    sort(results.begin(), results.end(),
         [](pair<uint64_t, string> p1, pair<uint64_t, string> p2) -> bool{
             return p2.second < p1.second;
         });
    return results;
}

bool DirectoryChecker::FileNameMatchAnyPrefix(
    const string& filePath, const vector<string>& prefixes)
{
    for (const string& prefix : prefixes)
    {
        if (StringUtil::startsWith(PathUtil::GetFileName(filePath), prefix))
        {
            return true;
        }
    }
    return false;
}

bool DirectoryChecker::CheckEqual(const DirectoryChecker::FileHashes& expectHashes,
                                   const DirectoryChecker::FileHashes& actualHashes)
{
    if (expectHashes.size() != actualHashes.size())
    {
        DirectoryChecker::PrintDifference(expectHashes, actualHashes);
        return false;
    }
    for (size_t i = 0; i < expectHashes.size(); ++i)
    {
        if (expectHashes[i] != actualHashes[i])
        {
            DirectoryChecker::PrintDifference(expectHashes, actualHashes);
            return false;
        }
    }
    return true;
}

void DirectoryChecker::PrintDifference(const DirectoryChecker::FileHashes& expectHashes,
                                       const DirectoryChecker::FileHashes& actualHashes)
{
    map<string, uint64_t> expectMap, actualMap;
    set<string> expectSet, actualSet;
    for (auto item : expectHashes)
    {
        expectMap.insert(make_pair(item.second, item.first));
        expectSet.insert(item.second);
    }
    for (auto item : actualHashes)
    {
        actualMap.insert(make_pair(item.second, item.first));
        actualSet.insert(item.second);
    }
    
    cout << "==================================================" << endl;
    cout << "Difference of EXPECT [" << expectHashes.size() << " files] "
            "vs ACTUAL [" << actualHashes.size() << " files] " << endl;

    vector<string> tmpVector;
    cout << "===== Files only appear in expect: =====" << endl;
    set_difference(expectSet.begin(), expectSet.end(), actualSet.begin(), actualSet.end(),
                   inserter(tmpVector, tmpVector.begin()));
    for (string file : tmpVector)
    {
        cout << file << endl;
    }
    
    cout << "===== Files only appear in actual: =====" << endl;
    tmpVector.clear();
    set_difference(actualSet.begin(), actualSet.end(), expectSet.begin(), expectSet.end(),
                   inserter(tmpVector, tmpVector.begin()));
    for (string file : tmpVector)
    {
        cout << file << endl;
    }
   
    cout << "===== Files with differect hash: =====" << endl;
    tmpVector.clear();
    set_intersection(actualSet.begin(), actualSet.end(), expectSet.begin(), expectSet.end(),
                     inserter(tmpVector, tmpVector.begin()));
    vector<string> sameFiles;
    for (string file : tmpVector)
    {
        if (expectMap[file] != actualMap[file])
        {
            cout << file << "  expect [" << expectMap[file] << "]"
                            "  actual [" << actualMap[file] << "]" << endl;
        }
        else
        {
            sameFiles.push_back(file);
        }
    }
    
    cout << "===== Same files: =====" << endl;
    for (string file : sameFiles)
    {
        cout << file << endl;
    }
    
    cout << "==================================================" << endl;
}

IE_NAMESPACE_END(test);
