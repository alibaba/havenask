#ifndef ISEARCH_DIRITERATOR_H
#define ISEARCH_DIRITERATOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>

BEGIN_HA3_NAMESPACE(util);

class DirIterator;
HA3_TYPEDEF_PTR(DirIterator);

class DirIterator {
public:
    DirIterator(const std::string& pathName);
    DirIterator(const std::vector<std::string>& fileList);
    ~DirIterator() {};

public:
    bool getNextFile(std::string& nextFileName);
    size_t getFileCount() const { return _fileNames.size(); }
    int getCurrentIndex() const { return _currentIdx; }
    std::string getCurrentFileName() {
        if (_currentIdx < 0 || (size_t)_currentIdx >= _fileNames.size()) {
            return "";
        }
        return _fileNames[_currentIdx];
    }

private:
    void getFileNameList(const std::string& dirName, std::vector<std::string> &fileNames);

private:
    std::vector<std::string> _fileNames;
    int _currentIdx;

private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(util);
#endif //ISEARCH_DIRITERATOR_H
