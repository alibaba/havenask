#include <ha3/util/BufferedFileReader.h>
#include <ha3/util/BufferedLocalFileReader.h>

BEGIN_HA3_NAMESPACE(util);
HA3_LOG_SETUP(util, BufferedFileReader);

BufferedFileReaderPtr BufferedFileReader::createBufferedFileReader(const std::string &fileName) {
    return BufferedFileReaderPtr(new BufferedLocalFileReader(fileName));
}

BufferedFileReader::BufferedFileReader(const std::string &fileName)
    : _fileName(fileName)
{ 
    _bufferSize = BLOCK_SIZE; 
    _startPos = 0;
    _endPos = 0;
    _buffer = new char[_bufferSize];
    _isEof = false;
}

BufferedFileReader::~BufferedFileReader() { 
    close();
}

void BufferedFileReader::seek(int64_t pos) {
    _startPos = _endPos = 0;
    doSeek(pos);
}

int64_t BufferedFileReader::getPosition() {
    return doGetPosition() - _endPos + _startPos;
}

void BufferedFileReader::close() {
    if(_buffer != NULL){
        delete[] _buffer;
        _buffer = NULL;
    }
}

ReadErrorCode BufferedFileReader::readLine(char *dest, size_t maxLength, 
        char delim, size_t &totalCount) {
    totalCount = 0;
    if (_isEof) {
        return RE_EOF;
    }

    _currentPos = _startPos;
    while (true) {
        if (_currentPos >= _endPos) {
            dumpSomeBuffer(dest, totalCount);
            _endPos = doRead(_buffer, _bufferSize);
            if (_endPos <= 0) { // end of file
                dest[totalCount] = '\0';
                if (totalCount > 0) {
                    return RE_NO_ERROR;
                } else {
                    _isEof = true;
                    return RE_EOF;
                }
            }
            _startPos = 0;
            _currentPos = 0;
        }
        if (totalCount + _currentPos - _startPos >= maxLength - 1) {
            // beyond the maxLength
            dumpSomeBuffer(dest, totalCount);
            _startPos = _currentPos;
            dest[totalCount] = '\0';
            return RE_BEYOND_MAXLENTH;
        }
        if (_buffer[_currentPos] == delim) {
            dumpSomeBuffer(dest, totalCount);
            _startPos = _currentPos + 1;
            dest[totalCount] = '\0';
            return RE_NO_ERROR;
        }
        ++_currentPos;
    }    
}

void BufferedFileReader::dumpSomeBuffer(char *dest, size_t &totalCount) {
    memcpy(dest + totalCount, _buffer + _startPos, _currentPos - _startPos);
    totalCount += _currentPos - _startPos;        
}

END_HA3_NAMESPACE(util);
