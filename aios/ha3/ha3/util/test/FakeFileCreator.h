#ifndef ISEARCH_FAKEFILECREATOR_H
#define ISEARCH_FAKEFILECREATOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>

BEGIN_HA3_NAMESPACE(util);

class FakeFileCreator
{
public:
    FakeFileCreator();
    ~FakeFileCreator();
private:
    FakeFileCreator(const FakeFileCreator &);
    FakeFileCreator& operator=(const FakeFileCreator &);
public:
    static bool prepareRecursiveDir(const std::string &testDir);
    static bool copyDirWithoutSvn(const std::string &from, 
                                  const std::string &to);
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(FakeFileCreator);

END_HA3_NAMESPACE(util);

#endif //ISEARCH_FAKEFILECREATOR_H
