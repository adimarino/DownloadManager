/**
 * A chunk of data file
 *
 * Contains an offset, bytes of data, and size
 */
class Chunk {
    private byte[] data;
    private long offset;
    private long size_in_bytes;

    Chunk(byte[] data, long offset, long size_in_bytes) {
        this.data = data != null ? data.clone() : null;
        this.offset = offset;
        this.size_in_bytes = size_in_bytes;
    }

    byte[] getData() {
        return data;
    }

    long getOffset() {
        return offset;
    }

    long getSizeInBytes() {
        return size_in_bytes;
    }
}
