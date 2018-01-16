import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.*;

public class IdcDm {
	static final int CHUNK_SIZE = 4096;

    /**
     * Receive arguments from the command-line, provide some feedback and start the download.
     *
     * @param args command-line arguments
     */
    public static void main(String[] args) {
        int numberOfWorkers = 1;
        Long maxBytesPerSecond = null;

        if (args.length < 1 || args.length > 3) {
            System.err.printf("usage:\n\tjava IdcDm URL [MAX-CONCURRENT-CONNECTIONS] [MAX-DOWNLOAD-LIMIT]\n");
            System.exit(1);
        } else if (args.length >= 2) {
            numberOfWorkers = Integer.parseInt(args[1]);
            if (args.length == 3)
                maxBytesPerSecond = Long.parseLong(args[2]);
        }

        String url = args[0];

        System.err.printf("Downloading");
        if (numberOfWorkers > 1)
            System.err.printf(" using %d connections", numberOfWorkers);
        if (maxBytesPerSecond != null)
            System.err.printf(" limited to %d Bps", maxBytesPerSecond);
        System.err.printf("...\n");

        DownloadURL(url, numberOfWorkers, maxBytesPerSecond);
    }

    /**
     * Initiate the file's metadata, and iterate over missing ranges. For each:
     * 1. Setup the Queue, TokenBucket, DownloadableMetadata, FileWriter, RateLimiter, and a pool of HTTPRangeGetters
     * 2. Join the HTTPRangeGetters, send finish marker to the Queue and terminate the TokenBucket
     * 3. Join the FileWriter and RateLimiter
     *
     * Finally, print "Download succeeded/failed" and delete the metadata as needed.
     *
     * @param url URL to download
     * @param numberOfWorkers number of concurrent connections
     * @param maxBytesPerSecond limit on download bytes-per-second
     */
    private static void DownloadURL(String url, int numberOfWorkers, Long maxBytesPerSecond) {
    	long size = getContentLength(url);
    	// Initiate DownloadableMetadata.
    	DownloadableMetadata downloadableMetadata = new DownloadableMetadata(url, size);
    	FileInputStream fileInputStream = null;
    	ObjectInputStream objectInputStream = null;
    	File file = null;
    	try {
			file = new File(downloadableMetadata.getMetaDataFilename());
			// If file exist (resuming downloading) update downloadableMetadata.
			if(file.exists()){
				fileInputStream = new FileInputStream(file);
				objectInputStream = new ObjectInputStream(fileInputStream);
				downloadableMetadata = (DownloadableMetadata) objectInputStream.readObject();
			}
		} catch (IOException | ClassNotFoundException e) {
			e.printStackTrace();
			System.err.println("Resuming download failed");
		} finally {
			try {
				if(fileInputStream != null){
					fileInputStream.close();
				}
				if(objectInputStream != null){
				objectInputStream.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
    	// Initiate Blocking Queue.
		ArrayBlockingQueue<Chunk> blockingQueue = new ArrayBlockingQueue<Chunk>((int) size);
		
		// Initiate FileWriter.
		FileWriter fileWriter = new FileWriter(downloadableMetadata, blockingQueue);

		maxBytesPerSecond = maxBytesPerSecond == null ? Long.MAX_VALUE : maxBytesPerSecond;

		// Initiate TokenBucket.
    	TokenBucket tokenBucket = new TokenBucket(maxBytesPerSecond);

    	// Initiate RateLimiter.
    	RateLimiter rateLimiter = new RateLimiter(tokenBucket, maxBytesPerSecond);

    	// Initiate and start fileWriter and rateLimiter threads.
    	Thread fileWriterT = new Thread(fileWriter);
    	Thread rateLimiterT = new Thread(rateLimiter);
    	fileWriterT.start();
    	rateLimiterT.start();

    	Thread[] threads = new Thread[numberOfWorkers];
    	
    	while(!downloadableMetadata.isCompleted()){

			Range rangeReader = downloadableMetadata.getMissingRange();
			long workerPartSize = rangeReader.getLength() / numberOfWorkers;
    		long startPos;
    		long endPos = 0L;

    		// Start threads with partial ranges (based on num of workers).
    		for(int i = 0; i < numberOfWorkers; i++){
    			startPos = i == 0 ? rangeReader.getStart() : endPos;
    			endPos = i == numberOfWorkers - 1 ? downloadableMetadata.getSize() - 1: startPos + workerPartSize;
    			
    			Range currentRange = new Range(startPos, endPos);
    			HTTPRangeGetter httpRangeGetter = new HTTPRangeGetter(url, currentRange, blockingQueue, tokenBucket);
    			threads[i] = new Thread(httpRangeGetter);
    			threads[i].start();
    			
    		}
    		
    		// Join all threads.
    		for(int i = 0; i < threads.length; i++){
    			try {
					threads[i].join();
				} catch (InterruptedException e) {
					e.printStackTrace();
					System.err.println("Download failed");
				}
    		}
    		
    		try {
				blockingQueue.put(new Chunk(new byte[0], -1, 0));
			} catch (InterruptedException e) {
				e.printStackTrace();
				System.err.println("Download failed");
			}
    		tokenBucket.terminate();
    		
    		try {
				fileWriterT.join();
				rateLimiterT.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
				System.err.println("Download failed");
			}
    		
    	}
    	
    	// Print download status.
    	if(downloadableMetadata.isCompleted()){
    		System.err.println("Download succeeded");
    		// Delete metadata file.
    		file.delete();
    	} else {
    		System.err.println("Download failed");
    	}   	
    }
    
    /**
     * Get Content-Length (file size) using Http HEAD request.
     * @param url
     * @return content-length (file size).
     * @throws IOException
     * @throws InterruptedException
     */
    public static long getContentLength(String url) {
    	URL urlToConnect;
        HttpURLConnection connection = null;
    	try{
    		urlToConnect = new URL(url);
            connection = (HttpURLConnection) urlToConnect.openConnection();
            connection.setRequestMethod("HEAD");
            connection.connect();
            return connection.getContentLength();
    	} catch (IOException e){
    		e.printStackTrace();
    	} finally {
    		connection.disconnect();
    	}
    	return 0L;
    }
}
