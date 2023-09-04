namespace {

template <typename HashTable>
bool hashTableInsert(HashTable& hashTale, typename HashTable::KeyType key, typename HashTable::ValueType value)
{
    autil::StringView cs((char*)&value, sizeof(value));
    return hashTale.Insert(key, cs);
}

template <typename HashTable>
bool hashTableDelete(HashTable& hashTale, typename HashTable::KeyType key, typename HashTable::ValueType value)
{
    autil::StringView cs((char*)&value, sizeof(value));
    return hashTale.Delete(key, cs);
}

} // namespace
