#ifndef NAVI_TESTDATA_H
#define NAVI_TESTDATA_H

#include "navi/engine/Data.h"
#include "navi/example/TestType.h"

namespace navi {

extern const std::string NAVI_TEST_DATA_PATH;
extern const std::string NAVI_TEST_PYTHON_HOME;

class HelloData : public Data {
public:
    HelloData();
    HelloData(const std::string &message);
    HelloData(const HelloData &helloData);
    const std::vector<std::string> &getData() const;
    void mark(const std::string &marker);
    void append(const std::string &value);
    void merge(HelloData *data);
    void show() const override;
    size_t size() const;
    size_t totalSize() const;
    void serialize(autil::DataBuffer &dataBuffer) const;
    void deserialize(autil::DataBuffer &dataBuffer);
private:
    std::vector<std::string> _dataVec;
};

class ErrorCommandData : public Data {
public:
    ErrorCommandData();
};

NAVI_TYPEDEF_PTR(HelloData);

}

#endif //NAVI_TESTDATA_H
