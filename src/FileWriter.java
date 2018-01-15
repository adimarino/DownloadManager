import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.util.concurrent.BlockingQueue;

/**
 * This class takes chunks from the queue, writes them to disk and updates the file's metadata.
 *
 * NOTE: make sure that the file interface you choose writes every update to the file's content or metadata
 *       synchronously to the underlying storage device.
 */
public class FileWriter implements Runnable {

    private final BlockingQueue<Chunk> chunkQueue;
    private DownloadableMetadata downloadableMetadata;
    private boolean isTerminated;

    FileWriter(DownloadableMetadata downloadableMetadata, BlockingQueue<Chunk> chunkQueue) {
        this.chunkQueue = chunkQueue;
        this.downloadableMetadata = downloadableMetadata;
        this.isTerminated = false;
    }

    private void writeChunks() throws IOException {
    	// Create new download file if needed.
    	File file = new File(this.downloadableMetadata.getFilename());
    	if (!file.exists()){
        	file.createNewFile();
        }
    	RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
    	
    	FileOutputStream metaDataFile = new FileOutputStream(this.downloadableMetadata.getMetaDataFilename());
    	long fileSize = this.downloadableMetadata.getSize();
    	long downloaded = this.downloadableMetadata.getTotalBytesWritten();
    	int percent = (int) (((double) downloaded / fileSize) * 100);
    	System.err.println("Downloaded " + percent + "%");
    	ObjectOutputStream metadataStream = null;
    	
    	// Take chunks until queueu is empty, and write them to dowloaded file and metadata.
    	try {
    		while(true){
    		
    			Chunk chunk = this.chunkQueue.take();
    			// Check if filewriter is terminated.
    			if(chunk.getOffset() == -1){
    				break;
    			}
				// Seek correct position for writing to downloaded file.
				randomAccessFile.seek(chunk.getOffset());
				// Write to dowonloaded file.
				randomAccessFile.write(chunk.getData(), 0, (int)chunk.getSizeInBytes());

				Range range = new Range(chunk.getOffset(), chunk.getOffset() + chunk.getSizeInBytes());
				this.downloadableMetadata.addRange(range);
				
				// Print current download percentage.
				downloaded = this.downloadableMetadata.getTotalBytesWritten();
				int currentPercent = (int) (((double) downloaded / fileSize) * 100);
				if(currentPercent != percent){
					System.err.println("Downloaded " + currentPercent + "%");
					percent = currentPercent;
				}
				
				metadataStream = new ObjectOutputStream(metaDataFile);
				metadataStream.writeObject(downloadableMetadata);
				
			
    		}
    		Thread.sleep(500);
    	} catch (InterruptedException e) {
			e.printStackTrace();
			System.err.println("Download failed");
		} finally {
			metadataStream.close();
			randomAccessFile.close();
			metaDataFile.close();
		}
    	
    	
    }
    
    /**
     * Mark the filewriter as terminated.
     */
    public void terminate() {
        this.isTerminated = true;
    }

    /**
     * Chcek if filewriter is terminated.
     * @return true if the filewriter is terminated, false otherwise.
     */
    boolean terminated() {
        return this.isTerminated;
    }
    
    @Override
    public void run() {
        try {
            this.writeChunks();
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("Download failed");
        }
    }
}
