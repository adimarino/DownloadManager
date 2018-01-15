import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.BlockingQueue;

/**
 * A runnable class which downloads a given url.
 * It reads CHUNK_SIZE at a time and writs it into a BlockingQueue.
 * It supports downloading a range of data, and limiting the download rate using a token bucket.
 */
public class HTTPRangeGetter implements Runnable {
    static final int CHUNK_SIZE = 4096;
    private static final int CONNECT_TIMEOUT = 500;
    private static final int READ_TIMEOUT = 2000;
    private final String url;
    private final Range range;
    private final BlockingQueue<Chunk> outQueue;
    private TokenBucket tokenBucket;

    HTTPRangeGetter(
            String url,
            Range range,
            BlockingQueue<Chunk> outQueue,
            TokenBucket tokenBucket) {
        this.url = url;
        this.range = range;
        this.outQueue = outQueue;
        this.tokenBucket = tokenBucket;
    }

    /**
     * Download chunks of the given range and put them in queue.
     * @throws IOException
     * @throws InterruptedException
     */
    private void downloadRange() throws IOException, InterruptedException {
    	
    	// Build range String for Http range property.
    	StringBuilder rangeProperty = new StringBuilder("bytes=");
        rangeProperty.append(this.range.getStart()).append("-").append(this.range.getEnd());
        
        // Build Http GET request.
        HttpURLConnection connection = buildRequest(rangeProperty.toString());
        
        // Connect (download).
        connection.connect();
        
        int response = 0;
        InputStream inputStream = null;
        int readSize = 0;
        
        try {
        	
        	this.tokenBucket.take(CHUNK_SIZE);
        	
        	connection.connect();
        	// Check response code.
        	response = connection.getResponseCode();
            if(response / 100 != 2){
            	System.err.println("Download Failed");
            	throw new IOException("Resonse Code: " + response);
            }
       
            inputStream = connection.getInputStream();
            byte[] buffer = new byte[CHUNK_SIZE];
            long offset = this.range.getStart();
            
            // Read from input stream and put chuncks in queue.
            while((readSize = inputStream.read(buffer)) != -1){
            	
            	this.tokenBucket.take(readSize);
            	Chunk chunk = new Chunk(buffer, offset, readSize);
            	outQueue.put(chunk);
            	offset += readSize;
            }
        } catch(Exception e) {
        	e.printStackTrace();
        	
        } finally {
        	if(inputStream != null){
        		inputStream.close();
        	}
        	connection.disconnect();
        }
    }
    
    /**
     * Build Http GET request for specific download range.
     * @param rangeProperty
     * @return HttpURLConnection
     * @throws IOException
     * @throws InterruptedException
     */
    private HttpURLConnection buildRequest(String rangeProperty) throws IOException, InterruptedException{
    	URL url = new URL(this.url);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("GET");
        connection.setReadTimeout(READ_TIMEOUT);
        connection.setConnectTimeout(CONNECT_TIMEOUT);
        connection.setRequestProperty("Range", rangeProperty);
        
        return connection;
    }

    @Override
    public void run() {
        try {
            this.downloadRange();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
            System.err.println("Download failed");
        }
    }
}
