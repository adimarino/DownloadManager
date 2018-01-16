import java.io.Serializable;

/**
 * Describes a simple range, with a start, an end, and a length
 */
class Range implements Serializable{
	private Long start;
    private Long end;

    Range(Long start, Long end) {
        this.start = start;
        this.end = end;
    }

    /**
     * Get start point of range.
     * @return start point of range.
     */
    Long getStart() {
        return start;
    }

    /**
     * Get end point of range.
     * @return end point of range.
     */
    Long getEnd() {
        return end;
    }

    /**
     * Get length of range.
     * @return length of range.
     */
    Long getLength() {
        return end - start + 1;
    }

}
