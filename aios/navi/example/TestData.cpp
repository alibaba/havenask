#include "navi/example/TestData.h"
#include <iostream>

namespace navi {

const std::string NAVI_TEST_DATA_PATH = "aios/navi/testdata/";
const std::string NAVI_TEST_PYTHON_HOME = "aios/navi/config_loader/python";

HelloData::HelloData()
    : Data(CHAR_ARRAY_TYPE_ID)
{
}

HelloData::HelloData(const std::string &message)
    : HelloData()
{
    _dataVec.push_back(message);
}

HelloData::HelloData(const HelloData &helloData)
    : HelloData()
{
    _dataVec = helloData._dataVec;
}

const std::vector<std::string> &HelloData::getData() const {
    return _dataVec;
}

void HelloData::mark(const std::string &marker) {
    for (auto &data : _dataVec) {
        if (data.empty()) {
            data = marker;
        } else {
            data = data + "+" + marker;
        }
    }
}

void HelloData::append(const std::string &value) {
    _dataVec.push_back(value);
}

void HelloData::merge(HelloData *data) {
    if (data) {
        _dataVec.insert(_dataVec.end(), data->_dataVec.begin(),
                        data->_dataVec.end());
    }
}

void HelloData::show() const {
    auto size = _dataVec.size();
    for (size_t i = 0; i < size; i++) {
        std::cout << "index: " << i << ": " << _dataVec[i] << std::endl;
    }
}

size_t HelloData::size() const {
    return _dataVec.size();
}

size_t HelloData::totalSize() const {
    size_t sum = 0;
    for (const auto &str : _dataVec) {
        sum += str.size();
    }
    return sum;
}

void HelloData::serialize(autil::DataBuffer &dataBuffer) const {
    dataBuffer.write(_dataVec);
}

void HelloData::deserialize(autil::DataBuffer &dataBuffer) {
    dataBuffer.read(_dataVec);
}


ErrorCommandData::ErrorCommandData()
    : Data(CHAR_ARRAY_TYPE_ID)
{
}


}
