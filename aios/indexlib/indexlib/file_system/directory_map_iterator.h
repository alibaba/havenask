#ifndef __INDEXLIB_DIRECTORY_MAP_ITERATOR_H
#define __INDEXLIB_DIRECTORY_MAP_ITERATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/util/path_util.h"

IE_NAMESPACE_BEGIN(file_system);

template <typename T>
class DirectoryMapIterator
{
public:
    typedef std::map<std::string, T> DirectoryMap;
    typedef typename DirectoryMap::iterator Iterator;

public:
    DirectoryMapIterator(DirectoryMap& pathMap, const std::string& dir)
        : mDirectoryMap(pathMap)
    {
        std::string tmpDir = util::PathUtil::NormalizePath(dir);
        if (!tmpDir.empty() && *tmpDir.rbegin() == '/') {
            tmpDir.resize(tmpDir.size() - 1);
        }
        mIt = mDirectoryMap.upper_bound(tmpDir);
        mDir = tmpDir + "/";
    }

    ~DirectoryMapIterator() {}

public:
    bool HasNext() const
    {
        if (mIt == mDirectoryMap.end())
        {
            return false;
        }

        const std::string& path = mIt->first;
        if (path.size() <= mDir.size()
            || path.compare(0, mDir.size(), mDir) != 0)
        {
            return false;
        }
        return true; 
    }

    Iterator Next() { return mIt++; }

    void Erase(Iterator it) { mDirectoryMap.erase(it); }

private:
    DirectoryMap& mDirectoryMap;
    std::string mDir;
    Iterator mIt;
};


IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_DIRECTORY_MAP_ITERATOR_H
