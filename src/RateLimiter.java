/**
 * A token bucket based rate-limiter.
 *
 * This class should implement a "soft" rate limiter by adding maxBytesPerSecond tokens to the bucket every second,
 * or a "hard" rate limiter by resetting the bucket to maxBytesPerSecond tokens every second.
 */
public class RateLimiter implements Runnable {
	
	private final static int SOFT_IMPL_WAITING_TIME = 1000;
	
    private final TokenBucket tokenBucket;
    private final Long maxBytesPerSecond;

    RateLimiter(TokenBucket tokenBucket, Long maxBytesPerSecond) {
        this.tokenBucket = tokenBucket;
        this.maxBytesPerSecond = maxBytesPerSecond;
    }

    @Override
    public void run() {
    	while(true){
    		if(this.tokenBucket.terminated()){
    			return;
    		}
    		
    		try {
				Thread.sleep(SOFT_IMPL_WAITING_TIME);
				this.tokenBucket.add(this.maxBytesPerSecond);
			} catch (InterruptedException e) {
				e.printStackTrace();
				System.err.println("Download failed");
			}
			
    	}
    }
}
