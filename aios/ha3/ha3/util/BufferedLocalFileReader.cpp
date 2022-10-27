#include <ha3/util/BufferedLocalFileReader.h>

using namespace std;

BEGIN_HA3_NAMESPACE(util);
 HA3_LOG_SETUP(util, BufferedLocalFileReader);

BufferedLocalFileReader::BufferedLocalFileReader(const string &fileName)
    : BufferedFileReader(fileName), _fin(fileName.c_str()) 
{
    _fin.seekg(0, ios::end);
    _fileLength = _fin.tellg();
    _fin.seekg(0, ios::beg);

}

BufferedLocalFileReader::~BufferedLocalFileReader() { 
    _fin.close();
}

int32_t BufferedLocalFileReader::doRead(void *buffer, int32_t size) {
    _fin.read((char *)buffer, size);
    return _fin.gcount();
}

void BufferedLocalFileReader::doSeek(int64_t pos) {
    _fin.seekg(pos, ios::beg);
}

int64_t BufferedLocalFileReader::doGetPosition(){
    if ((int64_t)_fin.tellg() == (int64_t)-1) {
        return _fileLength;
    } else {
        return _fin.tellg();
    }
}
END_HA3_NAMESPACE(util);

