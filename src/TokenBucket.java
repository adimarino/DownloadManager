/**
 * A Token Bucket (https://en.wikipedia.org/wiki/Token_bucket)
 *
 * This thread-safe bucket should support the following methods:
 *
 * - take(n): remove n tokens from the bucket (blocks until n tokens are available and taken)
 * - set(n): set the bucket to contain n tokens (to allow "hard" rate limiting)
 * - add(n): add n tokens to the bucket (to allow "soft" rate limiting)
 * - terminate(): mark the bucket as terminated (used to communicate between threads)
 * - terminated(): return true if the bucket is terminated, false otherwise
 *
 */
class TokenBucket {
	
	private static final int MAX_WAITING_TIME = 1000;
	
	private long tokenBucket;
	private long maxTokens;
	private boolean isTerminated;
	private long lastTimeAddedTokens;

    TokenBucket(long maxTokenPerMs) {
        this.tokenBucket = maxTokenPerMs;
        this.maxTokens = maxTokenPerMs;
        this.isTerminated = false;
        this.lastTimeAddedTokens = System.currentTimeMillis();
    }

    /**
     * Synchronously take tokens from the bucket,
     *  or waiting until tokens are available.
     * @param tokens
     */
    public synchronized void take(long tokens) {
    	while(this.tokenBucket < tokens){
    		long currentTime = System.currentTimeMillis();
    		try {
    			// Thread sleeps until next tokens add (or at most 1 second). 
				Thread.sleep(Math.min(MAX_WAITING_TIME, currentTime - this.lastTimeAddedTokens));
			} catch (InterruptedException e) {
				e.printStackTrace();
				System.err.println("Download failed");
			}
    	}
    	
    	this.tokenBucket -= tokens;
    }

    /**
     * Mark the bucket as terminated.
     */
    void terminate() {
        this.isTerminated = true;
    }

    /**
     * Chcek if token bucket is terminated.
     * @return true if the bucket is terminated, false otherwise.
     */
    boolean terminated() {
        return this.isTerminated;
    }

    /**
     * Set bucket with given number of tokens. 
     * @param tokens
     */
    public void set(long tokens) {
        this.tokenBucket = Math.min(tokens, this.maxTokens);
    }
    
    /**
     * Add given token number to the token bucket.
     * @param tokens
     */
    public void add(long tokens){
    	this.tokenBucket += tokens;
    }
}
